from collections import Counter
import json
import re
from tqdm import tqdm
from p_tqdm import p_map
from dotenv import load_dotenv
import os

load_dotenv()
CC_KOR_PATH = os.getenv('CC_KOR_PATH')
CC_KOR_800_PATH = os.getenv('CC_KOR_800_PATH')
CC_KOR_800_PREPROCESSED_PATH = os.getenv('CC_KOR_800_PREPROCESSED_PATH')

def show_hangul_prop(idx, props):
    with open(f'{CC_KOR_800_PATH}cc-kor-{str(idx).zfill(5)}.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    for i in data:
        props.append(len(re.findall('[가-힣]', i['content'])) / len(i['content']))
    return

def show_crlf_prop(idx, props):
    with open(f'{CC_KOR_800_PATH}cc-kor-{str(idx).zfill(5)}.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    for i in data:
        props.append(i['content'].count('\n') / len(i['content']))
    return

def show_length(idx, props):
    with open(f'{CC_KOR_800_PATH}cc-kor-{str(idx).zfill(5)}.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    for i in data:
        props.append(len(i['content']))
    return

def show_all():
    props = []
    for i in tqdm(range(800)):
        # show_hangul_prop(i, props)
        # show_crlf_prop(i, props)
        show_length(i, props)
    return props

def concat_files():
    for i in range(517, 518):
        data = []
        for j in range(100):
            with open(f'{CC_KOR_PATH}cc-kor-{str(i*100+j).zfill(5)}.json', 'r', encoding='utf-8') as f:
                data += json.load(f)
        with open(f'{CC_KOR_800_PATH}cc-kor-{str(i).zfill(5)}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)

def preprocess(idx):
    stopwords = ['약관', '개인정보', '출장', '상품', '고객님']
    with open(f'{CC_KOR_800_PATH}cc-kor-{str(idx).zfill(5)}.json', 'r', encoding='utf-8') as f:
        loaddata = json.load(f)
    savedata = []

    for j in loaddata:
        len_j = len(j['content'])
        if len_j < 256:
            continue
        foundchars = re.findall(r'(\n|약관|개인정보|출장|상품|고객님|[가-힣])', j['content'])
        foundcounts = Counter(foundchars)
        if foundcounts['\n'] / len_j > 0.0625:
            continue
        if foundcounts['약관'] + foundcounts['개인정보'] > 8:
            continue
        if foundcounts['출장'] > 8:
            continue
        if foundcounts['상품'] + foundcounts['고객님'] > 8:
            continue
        hangul_num = len(foundchars) - foundcounts['\n'] + sum(len(x)*foundcounts[x] for x in stopwords)
        if hangul_num / len_j < 0.25:
            continue

        duplicate = False
        for data in savedata:
            if len(data['content']) == len_j:
                if data['content'] == j['content']:
                    duplicate = True
                    break
        if not duplicate:
            savedata.append(j)

    with open(f'{CC_KOR_800_PREPROCESSED_PATH}cc-kor-{str(idx).zfill(5)}.json', 'w', encoding='utf-8') as f:
        json.dump(savedata, f, ensure_ascii=False)

if __name__ == '__main__':
    p_map(preprocess, range(800), num_cpus=8)