# 边狱公司
import json
import re
import os
from base_corpus import BaseCorpus

lang_dic = {
    'Assets/Resources_moved/Localize/en/': 'en_text',
    'Assets/Resources_moved/Localize/jp/': 'ja_text',
    'Assets/Resources_moved/Localize/kr/': 'ko_text',
    'Lang/LLC_zh-CN/': 'zh_text'
}
lang_pre = {'en_text': 'EN_', 'ja_text': 'JP_', 'ko_text': 'KR_', 'zh_text': ''}
lang_ex_code = {'en_text': r'[^a-zA-Z]',
                'ja_text': r'[^\u3040-\u30FF\u31F0-\u31FF]',
                'ko_text': r'[^\uAC00-\uD7AF]',
                'zh_text': r'[^\u4E00-\u9FA5]'}
sub_dirs = ['BattleAnnouncerDlg/', 'BgmLyrics/', 'EGOVoiceDig/', 'PersonalityVoiceDlg/', 'StoryData/', '']


def clean_content(lang, content):
    return re.sub(lang_ex_code[lang], '', content)


def is_valid(lang_content):
    for lang, content in lang_content.items():
        if not isinstance(content, str):
            return False
        content = clean_content(lang, content)
        if len(content) == 0:
            return False
    return True


class LimComCorpus(BaseCorpus):
    def load_contents(self):
        key_cnt = {}
        for path in sub_dirs:
            path_cnt = {}
            for lang_key, std_lang in lang_dic.items():
                f_path = self.path + '/' + lang_key + path
                files = os.listdir(f_path)
                for f in files:
                    if not f.endswith('.json'):
                        continue
                    with open(f_path + f, 'r', encoding='utf-8') as fp:
                        content = fp.read()
                    data = json.loads(content)
                    if len(data) == 0 or 'dataList' not in data:
                        continue
                    items = data['dataList']
                    for i in range(len(items)):
                        item = items[i]
                        props = item.keys()
                        if 'id' not in props:
                            rid = '_' + str(i)
                        else:
                            rid = str(item['id'])
                        key = path + f.replace(lang_pre[std_lang], '') + rid
                        for prop in props:
                            if prop != 'id':
                                key += prop
                            if key not in path_cnt:
                                path_cnt[key] = {}
                            path_cnt[key][std_lang] = item[prop]
            valid_cnt = {k: v for k, v in path_cnt.items() if is_valid(v)}
            key_cnt.update(valid_cnt)
        self.key_content = key_cnt


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--game_path', type=str, required=True, help='the corpus path of the game')

    args = parser.parse_args()
    game_path = args.game_path

    corpus = LimComCorpus(game_path, lang_dict=lang_dic)

    corpus.load_contents()
    corpus.export_corpus_new()
