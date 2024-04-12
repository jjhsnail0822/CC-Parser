from collections import Counter
import fasttext
from fastwarc.warc import ArchiveIterator
from fastwarc.stream_io import GZipStream
import gzip
import io
import json
from multiprocessing import Pool, Manager
import os
from os import path
import queue
import re
import requests
import sys
import time
import threading

PATH = '../../../cc/'
NUM_PROCESS = 16

class CCParser:
    def __init__(self, cc_main_id, start_idx):
        self.wet_paths = []
        self.headers = {
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        }
        self.cc_main_id = cc_main_id
        self.start_idx = start_idx
        self.threads = []

        # if there is no cc_main_id.paths, download it
        if not os.path.isfile(f'{PATH}wet-paths/{cc_main_id}.paths'):
            resp_data = requests.get(f'https://data.commoncrawl.org/crawl-data/{cc_main_id}/wet.paths.gz', headers=self.headers)
            if resp_data.status_code == 200:
                with open(f'{PATH}wet-paths/{cc_main_id}.paths', 'wb') as f:
                    f.write(gzip.decompress(resp_data.content))
                print(f'Downloaded: {cc_main_id}.paths')
            else:
                print(f'Failed to download: {cc_main_id}.paths')
                sys.exit(1)

        # create cc_main_id/ if it does not exist
        os.makedirs(f'{PATH}data/{cc_main_id}', exist_ok=True)

        # load cc_main_id.paths
        with open(f'{PATH}wet-paths/{cc_main_id}.paths', 'r') as f:
            for line in f:
                self.wet_paths.append(line.strip())
        print(f'Loaded: {cc_main_id}.paths')

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def download(self, idx):
        a = time.time()
        backoff_time = 1
        while True:
            try:
                resp_data = self.session.get(f'https://data.commoncrawl.org/{self.wet_paths[idx]}')
                if resp_data.status_code == 200:
                    print(f'Download time: {str(idx)} - {time.time()-a}')
                    print(f'Downloaded: {str(idx)} - {self.wet_paths[idx]}')
                    return resp_data.content
            except Exception as e:
                print(e)
            print(f'Retrying in {backoff_time} seconds: {str(idx)} - {self.wet_paths[idx]}')
            time.sleep(backoff_time)
            backoff_time *= 2
            self.session = requests.Session()
            self.session.headers.update(self.headers)
    
    def parse_and_save(self, idx, resp_content, model):
        a = time.time()
        dataset = []
        dataset_append = dataset.append
        resp_bytesio = io.BytesIO(resp_content)
        resp_stream = GZipStream(resp_bytesio)
        for record in ArchiveIterator(resp_stream, parse_http=False, func_filter=lambda r: 'kor' in str(r.headers.get('WARC-Identified-Content-Language'))):
            content = record.reader.read().decode('utf-8')
            length = int(record.headers.get('Content-Length'))
            if self.is_useless_content(content, length, model):
                continue
            dataset_append({'uri': record.headers.get('WARC-Target-URI'),
                            'date': record.headers.get('WARC-Date'),
                            'id': record.headers.get('WARC-Record-ID'),
                            'refer': record.headers.get('WARC-Refers-To'),
                            'sha1': record.headers.get('WARC-Block-Digest'),
                            'lang': record.headers.get('WARC-Identified-Content-Language'),
                            'len': length,
                            'content': content})
        resp_bytesio.close()
        with open(f'{PATH}data/{self.cc_main_id}/cc-kor-{str(idx).zfill(5)}.json', 'w', encoding='utf-8') as f:
            json.dump(dataset, f, ensure_ascii=False)
        print(f'Parse time: {str(idx)} - {time.time()-a}')
        print(f'Saved: {str(idx)} - {self.wet_paths[idx]}')
        return
    
    def is_useless_content(self, content, length, model):
        if model.predict(content.replace('\n',' '))[0][0] != '__label__ko':
            return True
        # foundchars = re.findall(r'(\n)', content)
        # foundcounts = Counter(foundchars)
        return False

    def worker(self, q):
        model = fasttext.load_model('lid.176.bin')
        while True:
            try:
                idx = q.get(block=False)
            except queue.Empty:
                break
            resp_content = self.download(idx)
            th = threading.Thread(target=self.parse_and_save, args=(idx, resp_content, model))
            th.start()
            self.threads.append(th)
            q.task_done()
        return

    # run if you want to complement the failed downloads
    def run_complement(self):
        model = fasttext.load_model('lid.176.bin')
        for idx in range(len(self.wet_paths)):
            if not path.exists(f'{PATH}data/{self.cc_main_id}/cc-kor-{str(idx).zfill(5)}.json'):
                resp_content = self.download(idx)
                self.parse_and_save(idx, resp_content, model)
        return

    def run(self):
        q = Manager().Queue()
        for idx in range(self.start_idx, len(self.wet_paths)):
            q.put(idx)
        with Pool(processes=NUM_PROCESS) as pool:
            for _ in range(NUM_PROCESS):
                pool.apply_async(self.worker, (q,))
            q.join()
            for th in self.threads:
                th.join()
        return

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python cc-parser.py cc-main-id start-idx')
        sys.exit(1)
    p = CCParser(str(sys.argv[1]), int(sys.argv[2]))
    p.run()
    # p.run_complement()