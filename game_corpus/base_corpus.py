from abc import abstractmethod
import warnings
import hashlib
import json
import time
import re

para_contents = {
    "行号": 0,
    "是否重复": False,
    "是否跨文件重复": False,
    "zh_text_md5": "",
    "zh_text": "",
    "en_text": "",
    "ar_text": "",
    "nl_text": "",
    "de_text": "",
    "eo_text": "",
    "fr_text": "",
    "he_text": "",
    "it_text": "",
    "ja_text": "",
    "pt_text": "",
    "ru_text": "",
    "es_text": "",
    "sv_text": "",
    "ko_text": "",
    "th_text": "",
    "id_text": "",
    "vi_text": "",
    "cht_text": "",
    "扩展字段": ""
}
default_json = {
    '文件名': '',
    '是否待查文件': False,
    '是否重复文件': False,
    '段落数': 0,
    '去重段落数': 0,
    '低质量段落数': 0,
    '行号': 0,
    '是否重复': False,
    '是否跨文件重复': False,
    'zh_text_md5': '',
    'zh_text': '',
    'en_text': '',
    'ar_text': '',
    'nl_text': '',
    'de_text': '',
    'eo_text': '',
    'fr_text': '',
    'he_text': '',
    'it_text': '',
    'ja_text': '',
    'pt_text': '',
    'ru_text': '',
    'es_text': '',
    'sv_text': '',
    'ko_text': '',
    'th_text': '',
    'id_text': '',
    'vi_text': '',
    'cht_text': '',
    '扩展字段': '',
    '时间': ''
}


def get_md5(content):
    if content:
        md5 = hashlib.md5()
        md5.update(content.encode('utf8'))
        return md5.hexdigest().lower()
    return ''


class BaseCorpus:
    """
    为减少语料转换代码开发的一个基类，继承该类后只需实现源语料到一个dict的映射即可
    """
    def __init__(self, path: str, lang_dict: dict[str, str] = None, export_duplicate: bool = True) -> None:
        # key_content: {key: {lang: content}}结构的dict，key为自定义的该语料键值，如自增id、文件路径+文件名+语料内置id，等等。
        # lang为该语料支持的语言，如英文en_text、简中zh_text，等等。content为该语言的语料，如"Chapter 1"、"第一章"等。
        self.key_content = {}
        self.lang_dict = lang_dict
        self.path = path
        self.output = path + '_parallel_corpus.jsonl'
        self.file_name = re.sub(r'.*?\\', '', re.sub(r'.*?/', '', self.output))
        self.export_duplicate = export_duplicate

    def init_lang_dic(self, dic: dict[str, str]) -> None:
        """
        初始化该语料所支持语言名称与标准名称的映射，
        如{"En": "en_text", "Cn": "zh_text"}、{"engus": "en_text", "zhocn": "zh_text"}等，
        如果未在实例初始化时定义，需在导出语料前定义
        """
        self.lang_dict = dic

    @abstractmethod
    def load_contents(self) -> None:
        # 根据该语料的文本格式转为本工具所需格式的转换代码，需自行实现
        pass

    def export_corpus(self):
        # 导出语料为标准格式，已废弃
        warnings.warn("导出语料为旧格式的jsonl，已废弃，请用export_corpus_new", DeprecationWarning, stacklevel=2)
        para = 0
        paras = []
        dup_count = 0
        lq_count = 0
        zh_md5 = set()
        for k, v in self.key_content.items():
            curr_para = para_contents.copy()
            ex_lang = {}
            for lang, content in v.items():
                if content:
                    if lang in self.lang_dict.values():
                        curr_para[lang] = content
                    else:
                        ex_lang.update({lang: content})
            # 繁简中文都没有的就不要了
            if not curr_para['zh_text'] and not curr_para['cht_text']:
                continue
            curr_para['zh_text_md5'] = get_md5(curr_para['zh_text'])
            is_dup = False
            if curr_para['zh_text_md5'] in zh_md5:
                is_dup = True
                if self.export_duplicate:
                    dup_count += 1
            zh_md5.add(curr_para['zh_text_md5'])
            if not is_dup or self.export_duplicate:
                para += 1
                curr_para['行号'] = para
                if len(ex_lang) > 0:
                    curr_para['扩展字段'] = json.dumps(ex_lang, ensure_ascii=False)
                # 无简中或英文的认为是低质量语料
                if not curr_para['zh_text'] or not curr_para['en_text']:
                    lq_count += 1
                    # print(curr_para['zh_text_md5'])
                paras.append(curr_para)

        out_json = {
            '文件名': self.file_name,
            '是否待查文件': False,
            '是否重复文件': False,
            '段落数': para,
            '去重段落数': dup_count,
            '低质量段落数': lq_count,
            '段落': paras,
            '扩展字段': '',
            '时间': time.strftime('%Y%m%d')
        }
        with open(self.output, 'w', encoding='utf8') as f:
            f.write(json.dumps(out_json, ensure_ascii=False, indent=None))

    def export_corpus_new(self) -> None:
        # 导出语料为新的标准格式
        para = 0
        paras = []
        dup_count = 0
        lq_count = 0
        zh_md5 = set()
        for k, v in self.key_content.items():
            curr_para = para_contents.copy()
            ex_lang = {}
            for lang, content in v.items():
                if content:
                    if lang in self.lang_dict.values():
                        curr_para[lang] = content
                    else:
                        ex_lang.update({lang: content})
            # 繁简中文都没有的就不要了
            if not curr_para['zh_text'] and not curr_para['cht_text']:
                continue
            curr_para['zh_text_md5'] = get_md5(curr_para['zh_text'])
            is_dup = False
            if curr_para['zh_text_md5'] in zh_md5:
                is_dup = True
                if self.export_duplicate:
                    dup_count += 1
            zh_md5.add(curr_para['zh_text_md5'])
            if not is_dup or self.export_duplicate:
                para += 1
                curr_para['行号'] = para
                if len(ex_lang) > 0:
                    curr_para['扩展字段'] = json.dumps({'other_texts': ex_lang}, ensure_ascii=False)
                else:
                    curr_para['扩展字段'] = '{}'
                    # 无简中或英文的认为是低质量语料
                if not curr_para['zh_text'] or not curr_para['en_text']:
                    lq_count += 1
                    # print(curr_para['zh_text_md5'])
                paras.append(curr_para)
        output = [self.compose_json(p, len(paras), dup_count, lq_count) for p in paras]
        out_json = [json.dumps(x, ensure_ascii=False) for x in output]
        with open(self.output, 'w', encoding='utf8') as f:
            f.write('\n'.join(out_json))

    def compose_json(self, para: dict, para_count: int, dup_count: int, lq_count: int) -> dict:
        out_json = default_json.copy()
        out_json['文件名'] = self.file_name
        out_json['段落数'] = para_count
        out_json['去重段落数'] = dup_count
        out_json['低质量段落数'] = lq_count
        out_json.update(para)
        out_json['时间'] = time.strftime('%Y%m%d')
        return out_json
