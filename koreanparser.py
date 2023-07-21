from fastwarc.warc import ArchiveIterator
from fastwarc.stream_io import GZipStream
import io
import json
from multiprocessing import Process
import requests
import sys
import time

# a parser that extracts korean data from commoncrawl
class KoreanParser:
    # initialize the parser
    # you need to download wet.paths from commoncrawl
    def __init__(self):
        self.warc_paths = []
        with open('wet.paths', 'r') as f:
            for line in f:
                self.warc_paths.append(line.strip())

    # run the parser
    # start: the index of warc_paths to start
    def run(self, start):
        save_kor_process = Process()
        for idx in range(start, len(self.warc_paths)):
            # download the warc file
            resp_data = requests.get(f'https://data.commoncrawl.org/{self.warc_paths[idx]}')
            # if the download fails, retry in a new process
            if resp_data.status_code != 200:
                with open('failed.txt', 'a') as f:
                    f.write(f'{str(idx)}\n')
                run_single_process = Process(target=self.run_single, args=(idx,), daemon=True)
                run_single_process.start()
                continue
            print(f'Downloaded: {str(idx)} - {self.warc_paths[idx]}')

            # wait until the previous save_kor_process is finished
            if save_kor_process.is_alive():
                save_kor_process.join()

            # parse the downloaded warc file in a new process
            save_kor_process = Process(target=self.save_kor, args=(idx, resp_data, f'cc-kor-{str(idx).zfill(5)}.json'), daemon=True)
            save_kor_process.start()

    # run the parser for a single warc file (retry if the download fails)
    def run_single(self, idx):
        backoff_time = 60
        while True:
            print(f'Retrying in {backoff_time//60} minutes: {str(idx)} - {self.warc_paths[idx]}')
            time.sleep(backoff_time)
            resp_data = requests.get(f'https://data.commoncrawl.org/{self.warc_paths[idx]}')
            if resp_data.status_code == 200:
                break
            else:
                backoff_time *= 2
        print(f'Downloaded: {str(idx)} - {self.warc_paths[idx]}')
        
        self.save_kor(idx, resp_data, f'cc-kor-{str(idx).zfill(5)}.json')

        with open('retry-and-success.txt', 'a') as f:
            f.write(f'{str(idx)}\n')

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

p = KoreanParser()
p.run(int(sys.argv[1]))