from fastwarc.warc import ArchiveIterator
from fastwarc.stream_io import GZipStream
import io
import json
from multiprocessing import Process
from os import path
import requests
import sys
import time

# a parser that extracts korean data from commoncrawl
class KoreanParser:
    # initialize the parser
    # you need to download wet.paths from commoncrawl
    def __init__(self):
        self.warc_paths = []
        self.headers = {
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }
        with open('wet.paths', 'r') as f:
            for line in f:
                self.warc_paths.append(line.strip())

    # run the parser with multiprocessing
    # start: the index of warc_paths to start
    def run_multiprocessing(self, num_process, start):
        processes = []
        for idx in range(start, len(self.warc_paths)):
            time.sleep(1)
            while len(processes) >= num_process:
                for p in processes[:]:
                    if not p.is_alive():
                        processes.remove(p)
                time.sleep(1)

            p = Process(target=self.run_single, args=(idx,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    # run the parser for a single warc file (retry if the download fails)
    def run_single(self, idx):
        backoff_time = 60
        while True:
            resp_data = requests.get(f'https://data.commoncrawl.org/{self.warc_paths[idx]}', headers=self.headers)
            if resp_data.status_code == 200:
                print(f'Downloaded: {str(idx)} - {self.warc_paths[idx]}')
                break
            else:
                if backoff_time == 60:
                    with open('failed.txt', 'a') as f:
                        f.write(f'{str(idx)}\n')
                print(f'Retrying in {backoff_time//60} minutes: {str(idx)} - {self.warc_paths[idx]}')
                time.sleep(backoff_time)
                backoff_time *= 2
        
        # parse the downloaded warc file in a new process
        save_kor_process = Process(target=self.save_kor, args=(idx, resp_data, f'cc-kor-{str(idx).zfill(5)}.json'))
        save_kor_process.start()

        if backoff_time > 60:
            with open('retry-and-success.txt', 'a') as f:
                f.write(f'{str(idx)}\n')

    # run if you want to complement the failed downloads
    def run_complement(self):
        for idx in range(len(self.warc_paths)):
            if not path.exists(f'out/cc-kor-{str(idx).zfill(5)}.json'):
                self.run_single(idx)
    
    # save korean data from the response
    def save_kor(self, idx, resp, save_file_name):
        dataset = []
        dataset_append = dataset.append
        resp_bytesio = io.BytesIO(resp.content)
        resp_stream = GZipStream(resp_bytesio)
        for record in ArchiveIterator(resp_stream, parse_http=False, func_filter=lambda r: 'kor' in str(r.headers.get('WARC-Identified-Content-Language'))):
            dataset_append({'uri': record.headers.get('WARC-Target-URI'),
                            'date': record.headers.get('WARC-Date'),
                            'id': record.headers.get('WARC-Record-ID'),
                            'refer': record.headers.get('WARC-Refers-To'),
                            'sha1': record.headers.get('WARC-Block-Digest'),
                            'lang': record.headers.get('WARC-Identified-Content-Language'),
                            'len': record.headers.get('Content-Length'),
                            'content': record.reader.read().decode('utf-8')})
        resp_bytesio.close()
        
        # save the parsed data as json file
        with open(f'out/{save_file_name}', 'w', encoding='utf-8') as f:
            json.dump(dataset, f, ensure_ascii = False)

        print(f'Done: {str(idx)} - {self.warc_paths[idx]}')

if __name__ == '__main__':
    p = KoreanParser()
    #p.run_multiprocessing(int(sys.argv[1]), int(sys.argv[2]))
    p.run_complement()