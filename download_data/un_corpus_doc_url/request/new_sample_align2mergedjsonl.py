import os
from pathlib import Path
import hashlib
import json
import re

import datasets
import datetime

import const

ALL_SOURCE_LANGS = ('es', 'zh', 'fr', 'ru', 'ar', 'de')
TARGET_LANG = 'en'
INPUT_DIR_ALIGNMENT = const.ALIGN_OUTPUT_DIR # align2_poc.py输出
INPUT_ORIGINAL_TEXT_EXPORTED = const.CONVERT_DATASET_CACHE_DIR # 翻译的上一步即文档转文本的输出
OUTPUT_FILE_INFO = const.BLOCKWISE_JSONL_OUTPUT_DIR # 文件信息输出

TODAY_STR = datetime.datetime.now().strftime("%Y%m%d")

print("TODAY_STR:",TODAY_STR)

def dsu_find(dsu: dict, x):
    dsu.setdefault(x, x)
    if dsu[x] == x:
        return x
    dsu[x] = dsu_find(dsu, dsu[x])
    return dsu[x]

def dsu_union(dsu: dict, x, y):
    dsu[dsu_find(dsu, x)] = dsu_find(dsu, y)

def clean_paragraph(paragraph):
    lines = paragraph.split('\n')
    para = ''
    table = []

    for line in lines:
        line = line.strip()

        # 表格线或其他分割线
        if re.match(r'^\+[-=+]+\+|-+|=+|_+$', line):
            if not para.endswith('\n'):
                para += '\n'
            if len(table) > 0:
                para += '\t'.join(table)
                table = []
        # 表格中的空行
        elif re.match(r'^\|( +\|)+$', line):
            para += '\t'.join(table) + ' '
            table = []
        # 表格中的内容行
        elif re.match(r'^\|([^|]+\|)+$', line):
            if len(table) == 0:
                table = line[1:-2].split('|')
            else:
                arr = line[1:-2].split('|')
                if len(arr) == len(table):
                    table = [table[i].strip() + arr[i].strip() for i in range(len(table))]
                elif len(arr) > len(table):
                    table = [table[i].strip() + arr[i].strip() if i < len(table) else arr[i].strip() for i in range(len(arr))]
                else:
                    table = [table[i].strip() + arr[i].strip() if i < len(arr) else table[i].strip() for i in range(len(table))]
        # 正文内容
        else:
            para += ' ' + line
    if len(table) > 0:
        if not para.endswith('\n'):
            para += '\n'
        para += '\t'.join(table)
    return re.sub(r'[ \t]{2,}', ' ', re.sub(r'\n{2,}', '\n', para)).strip()


def gen_func():
    all_align_ds = {}
    all_align_idx = {}
    all_align_idx_map = {} # rec -> idx
    overall_blocks_count = 0 
    for src_lang in ALL_SOURCE_LANGS:
        all_align_ds[src_lang] = datasets.load_from_disk(INPUT_DIR_ALIGNMENT / f"{src_lang}2{TARGET_LANG}")
        all_align_idx[src_lang] = 0
        all_align_idx_map[src_lang] = {}
        dl = len(all_align_ds[src_lang])
        for idx, row in enumerate(all_align_ds[src_lang]):
            if (idx & 0xffff) == 0xffff:
                print(idx, '/', dl)
            rec = row['record']
            all_align_idx_map[src_lang].setdefault(rec, []).append(idx)
    with OUTPUT_FILE_INFO.open('w', encoding='utf-8') as f:
        for idx, row in enumerate(datasets.load_from_disk(INPUT_ORIGINAL_TEXT_EXPORTED)):
            rec = row['record']
            print(idx, rec)
            dsu = {}
            clean_text = {TARGET_LANG: list(filter(bool, (clean_paragraph(x) for x in re.split('\n\n', row[TARGET_LANG]))))}
            for src_lang, ds in all_align_ds.items():
                clean_text[src_lang] = list(filter(bool, (clean_paragraph(x) for x in re.split('\n\n', row[src_lang]))))
                # idx = all_align_idx[src_lang]
                for idx in all_align_idx_map[src_lang].get(rec, []):
                # while idx < len(ds) and ds[idx]['record'] == rec:
                    edge_set = ds[idx]['clean_para_index_set_pair']
                    src_paras, tgt_paras = edge_set.split('|')
                    src_paras = src_paras.split(',')
                    tgt_paras = tgt_paras.split(',')
                    dsu_union(dsu, (src_lang, int(src_paras[0])), (TARGET_LANG, int(tgt_paras[0])))
                    for src_para in src_paras:
                        dsu_union(dsu, (src_lang, int(src_para)), (src_lang, int(src_paras[0])))
                    for tgt_para in tgt_paras:
                        dsu_union(dsu, (TARGET_LANG, int(tgt_para)), (TARGET_LANG, int(tgt_paras[0])))
                    # idx += 1
                    # all_align_idx[src_lang] += 1
            blocks = {}
            for k, v in dsu.items(): # 这一步中，只有单语种的文件会因为不会有连边，而被舍弃，考虑到这部分数据对于平行语料来说没有用，不打算挽留这些数据
                dsu[k] = dsu_find(dsu, v)
                blocks.setdefault(dsu[k], []).append(k)
            
            for idx, keylist in enumerate(blocks.values()):
                para_text_buffer = {}
                keylist.sort()
                for key in keylist:
                    lang, para_idx = key
                    para_text_buffer.setdefault(lang, []).append(clean_text[lang][para_idx])
                for k, v in para_text_buffer.items():
                    para_text_buffer[k] = '\n\n'.join(v)
                output_block_info = {
                    '文件名': rec,
                    '扩展字段': r'{}',
                    "时间": TODAY_STR,
                    'zh_text': para_text_buffer.get('zh',''),
                    'en_text': para_text_buffer.get('en',''),
                    'ar_text': para_text_buffer.get('ar',''),
                    'de_text': para_text_buffer.get('de',''),
                    'fr_text': para_text_buffer.get('fr',''),
                    'ru_text': para_text_buffer.get('ru',''),
                    'es_text': para_text_buffer.get('es',''),
                }
                f.write(json.dumps(output_block_info, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    if os.path.exists(OUTPUT_FILE_INFO):
        os.remove(OUTPUT_FILE_INFO)
    gen_func()