"""
mnbvc 平行语料小组的通用后处理脚本。每个语料文件都应该在数据检查器之前运行此脚本，否则语料文件将被拒绝发布。
- 将旧式平行语料转换为新式平行语料
- 自动填充几个能够根据给定段落计算出来的字段
- 验证扩展字段（仅接受 json 格式）。
- 完成基本的自动去重、删除空行

老版本平行语料样例（注意：实际语料一行为一个有效json，不会在json中穿插换行符，这里做换行和缩进仅作为展示用）：
{
    "文件名": "Terraria-workshop-localization_test2.jsonl",
    "是否待查文件": false,
    "是否重复文件": false,
    "段落数": 17944,
    "去重段落数": 0,
    "低质量段落数": 0,
    "段落": [
        {
            "行号": 1,
            "是否重复": false,
            "是否跨文件重复": false,
            "it_text": "",
            "zh_text": "正在生成海洋沙",
            "en_text": "Generating ocean sand",
            "ar_text": "",
            "nl_text": "",
            "de_text": "",
            "eo_text": "",
            "fr_text": "Génération du sable de l'océan",
            "he_text": "",
            "ja_text": "",
            "pt_text": "Gerando areia do oceano",
            "ru_text": "Создание песка в океане",
            "es_text": "",
            "sv_text": "",
            "ko_text": "",
            "th_text": "",
            "other1_text": "",
            "other2_text": "",
            "id_text":"",
            "cht_text":"",
            "vi_text":"",
            "扩展字段": "{\"other_texts\": {\"cs\": \"Generování mořského písku\", \"pl\": \"Generowanie piasku morskiego\", \"hu\": \"Tengeri homok elhelyezése\", \"uk\": \"Генерація океанського піску\", \"tr\": \"Okyanus kumu üretme\"}}",
            "时间": "20240316",
            "zh_text_md5": "b656579704c6ca5acc29f2aa36159ce2"
        }
    ],
    "扩展字段": "{\"other_texts_iso_map\": {\"cs\": \"捷克语\", \"pl\": \"波兰语\", \"hu\": \"匈牙利语\", \"uk\": \"乌克兰语\", \"tr\": \"土耳其语\"}}",
    "时间": "20240316"
}

升级后的新版本语料样例：

{
    "文件名": "Terraria-workshop-localization_test2.jsonl",
    "是否待查文件": false,      【不用手填】
    "是否重复文件": false,      【不用手填】
    "段落数": 17944,            【不用手填】
    "去重段落数": 0,            【不用手填】
    "低质量段落数": 0,          【不用手填】
    "行号": 1,                  【不用手填】
    "是否重复": false,          【不用手填】
    "是否跨文件重复": false,    【不用手填】
    "it_text": "",
    "zh_text": "正在生成海洋沙",
    "en_text": "Generating ocean sand",
    "ar_text": "",
    "nl_text": "",
    "de_text": "",
    "eo_text": "",
    "fr_text": "Génération du sable de l'océan",
    "he_text": "",
    "ja_text": "",
    "pt_text": "Gerando areia do oceano",
    "ru_text": "Создание песка в океане",
    "es_text": "",
    "sv_text": "",
    "ko_text": "",
    "th_text": "",
    "id_text":"",
    "cht_text":"",
    "vi_text":"",
    "扩展字段": "{\"other_texts\": {\"cs\": \"Generování mořského písku\", \"pl\": \"Generowanie piasku morskiego\", \"hu\": \"Tengeri homok elhelyezése\", \"uk\": \"Генерація океанського піску\", \"tr\": \"Okyanus kumu üretme\"}}",
    "时间": "20240316",
    "zh_text_md5": "b656579704c6ca5acc29f2aa36159ce2"   【不用手填】
}

弃用other1_text、other2_text，展平段落，用段落内层的扩展字段替换外层文件级扩展字段，对于文件级的信息，按段落冗余一份，以文件名为唯一过滤依据

"""
from collections import Counter
import json
import hashlib
import argparse
import copy
import os
from pathlib import Path
from io import BytesIO
import pickle

parser = argparse.ArgumentParser(description='''Common post-process script for parallel corpus mnbvc. Every corpus file should run this script before datachecker, or the corpus file cannot be accepted then published.
    - convert old-style parallel corpus to new-style parallel corpus
    - autofill common fields
    - validate extension field (only json format is accepted).
    - auto deduplicate
    - delete empty lines
''')
parser.add_argument('input', type=str, help='The input file path', nargs='?')
parser.add_argument('-d', '--directory', type=str, help='Process a directory instead of a single file')
parser.add_argument('-a', '--all_directory_mode', type=str, help='Read all files under given directory, then generate output file using given filename specify by this arg')
parser.add_argument('-v', '--verbose', action='store_true', help='Print deduplication info')
parser.add_argument('-dr', '--disable_rename', action='store_true', help='Disable auto assign json `filename` field to its file name')
parser.add_argument('-dc', '--disable_opencc_convert', action='store_true', help='Disable chinese Conversion by BYVoid/OpenCC')
parser.add_argument('-dbg', '--debug', action='store_true', help='Print debug info')
parser.add_argument('-b', '--bytes_limit', type=int, default=536870912, help='Specify the upper limit each output jsonl file in bytes')
# parser.add_argument('-ea', '--enable_assert', action='store_true', help='Enable assertions in the script')
# parser.add_argument('-da', '--disable_auto_dedup', action='store_true', help='Disable auto deduplicate and empty line elimination')

args = parser.parse_args()
del parser
is_first = True

KEEP_KEYS = [
    "行号",
    "是否重复",
    "是否跨文件重复",
    "it_text",
    "zh_text",
    "en_text",
    "ar_text",
    "nl_text",
    "de_text",
    "eo_text",
    "fr_text",
    "he_text",
    "ja_text",
    "pt_text",
    "ru_text",
    "es_text",
    "sv_text",
    "ko_text",
    "th_text",
    "id_text",
    "cht_text",
    "vi_text",
    "扩展字段",
    "时间",
    "zh_text_md5",
]

LANG_FIELDS = [
    "it_text",
    "zh_text",
    "en_text",
    "ar_text",
    "nl_text",
    "de_text",
    "eo_text",
    "fr_text",
    "he_text",
    "ja_text",
    "pt_text",
    "ru_text",
    "es_text",
    "sv_text",
    "ko_text",
    "th_text",
    "id_text",
    "cht_text",
    "vi_text",
]

NEW_STYLE_FIELDS = [
    "文件名",
    "是否待查文件",
    "是否重复文件",
    "段落数",
    "去重段落数",
    "低质量段落数",
    "行号",
    "是否重复",
    "是否跨文件重复",
    "it_text",
    "zh_text",
    "en_text",
    "ar_text",
    "nl_text",
    "de_text",
    "eo_text",
    "fr_text",
    "he_text",
    "ja_text",
    "pt_text",
    "ru_text",
    "es_text",
    "sv_text",
    "ko_text",
    "th_text",
    "id_text",
    "cht_text",
    "vi_text",
    "扩展字段",
    "时间",
    "zh_text_md5",
]

# 文件统计相关的走全局变量
# 以文件名为主键，不同的文件名不共享行号、行结构、中文去重计数
first_warn_unk_key = set()
first_warn_other_texts_key_check = set()
filename2zh_text_digest = {}
filename2low_quality_count = Counter()
filename2linecount = Counter()
valid_line_idx_set = set()
filename2zh_text_dedup_count = Counter()
filename2linedigest = {}

def validate_ext_fields(data: dict, disable_ext_field_check: bool):
    if data.get('扩展字段') is None:
        data['扩展字段'] = data.pop('拓展字段', r'{}')
    if data['扩展字段'] == '':
        data['扩展字段'] = r'{}'
    try:
        ext_field = json.loads(data['扩展字段'])
        if disable_ext_field_check:
            data['扩展字段'] = json.dumps(ext_field, ensure_ascii=False, sort_keys=True)
            return
        accepted_fields = {}
        if 'other_texts' in ext_field:
            other_texts_field = ext_field.pop('other_texts')
            for k, v in other_texts_field.items():
                if len(k) != 2 or not k.islower():
                    if k not in first_warn_other_texts_key_check:
                        first_warn_other_texts_key_check.add(k)
                        print("【警告】other_texts含有key名可能不合ISO 639-1规范的语种双字母缩写，请向工作群报告:", k)
            accepted_fields['other_texts'] = other_texts_field
        if 'k' in ext_field:
            k_field = ext_field.pop('k')
            accepted_fields['k'] = k_field
        for unknown_key, val in ext_field.items():
            if unknown_key not in first_warn_unk_key:
                first_warn_unk_key.add(unknown_key)
                print("【警告】扩展字段含有尚未定义的字段，请向工作群报告:", unknown_key)
            accepted_fields[unknown_key] = val # 打印警告信息，但是允许收录
        ext_field.clear()
        data['扩展字段'] = json.dumps(accepted_fields, ensure_ascii=False, sort_keys=True)
    except Exception as e:
        print("【错误】扩展字段并非有效json字符串：", data['扩展字段'])
        exit(1)

def gen_new_style_line(file_path: Path, disable_ext_field_check: bool):
    with open(file_path, "r", encoding='utf-8') as fi:
        # fic = fi.read() # 直接读40G文件报 Memory Error 了
        # $ wc -l dual_ass.jsonl
        # 92917622 dual_ass.jsonl
        linecounter = 0
        for linestr in fi:
            linecounter += 1
            if args.debug and linecounter % 100000 == 0: print("READING FILE:", linecounter)
            linestr = linestr.strip()
            if not linestr: continue
            data: dict = json.loads(linestr)
            if not args.disable_rename:
                data['文件名'] = file_path.name # 对于游戏语料，这里强制要求文件名等于jsonl内部文件名
            validate_ext_fields(data, disable_ext_field_check)
            if '段落' in data: # 旧版语料
                for pid, p in enumerate(data['段落']):
                    if '时间' not in p or not p['时间']:
                        p['时间'] = data['时间']
                    if p.get('扩展字段') is None:
                        p['扩展字段'] = p.pop('拓展字段', r'{}')
                    if p['扩展字段'] == '':
                        p['扩展字段'] = r'{}'
                    assert p['other1_text'] == '', f"【错误】段落{p['行号']}中存在other1_text字段 => {p}，请确认具体是哪种语言，并填入扩展字段中"
                    assert p['other2_text'] == '', f"【错误】段落{p['行号']}中存在other2_text字段 => {p}，请确认具体是哪种语言，并填入扩展字段中"
                    try:
                        ext_field = json.loads(p['扩展字段'])
                        p['扩展字段'] = json.dumps(ext_field, ensure_ascii=False, sort_keys=True)
                    except Exception as e:
                        print("【错误】扩展字段并非有效json字符串：", p)
                        exit(1)
                    for lang_field in LANG_FIELDS:
                        p.setdefault(lang_field, "")
                data_cloned = copy.deepcopy(data)
                data_cloned.pop('段落')
                for pid, p in enumerate(data['段落']):
                    for k in KEEP_KEYS:
                        data_cloned[k] = p[k]
                    cht_text: str = data_cloned["cht_text"]
                    zh_text: str = data_cloned["zh_text"]
                    if not zh_text and cht_text and not args.disable_opencc_convert:
                        import opencc
                        converter = opencc.OpenCC(config="t2s")
                        zh_text = converter.convert(cht_text)
                        data_cloned["zh_text"] = zh_text
                    yield data_cloned
            else:
                cht_text: str = data["cht_text"]
                zh_text: str = data["zh_text"]
                if not zh_text and cht_text and not args.disable_opencc_convert:
                    import opencc
                    converter = opencc.OpenCC(config="t2s")
                    zh_text = converter.convert(cht_text)
                    data["zh_text"] = zh_text
                yield data # 需要避免把json序列化之后的东西保存下来，可能会有字符串形式的表示的数十倍大

def process_file(file_path: Path):
    global is_first
    out_file_dir = file_path.parent / "jsonl_reworked"
    if is_first:
        if os.path.exists(out_file_dir):
            print(f"请确保{out_file_dir}目录为空，否则其内容可能会被覆盖。如不希望请直接结束本程序。")
            if input("请输入Y以确认继续进行:") != 'Y':
                print("程序退出...")
                exit(0)
        else:
            os.makedirs(out_file_dir)
        is_first = False
    del out_file_dir
    for lineidx, linejson in enumerate(gen_new_style_line(file_path, False)):
        #######去除空行#######
        line_dedup_set = set()
        for lang_field in LANG_FIELDS:
            linejson[lang_field] = linejson[lang_field].strip()
            line_dedup_set.add(linejson[lang_field])
        line_dedup_set.discard("")
        if len(line_dedup_set) <= 1:
            if args.verbose:
                print('【段落去冗余】为空或不同语种字段全一致的段落:',linejson)
            continue
        #######去除空行#######
        linejsonfilename = linejson['文件名']
        #######文件级去重#######，去除所有LANG_FIELDS加上扩展字段，完全一致的段落，如[{"en_text":"Fine","zh_text":"好"},{"en_text":"Fine","zh_text":"好"}],这种重复只保留第一次出现的那段
        dedup_str_set: set = filename2linedigest.setdefault(linejsonfilename, set())
        dedup_dict = {'扩展字段':linejson['扩展字段']}
        for lang_field in LANG_FIELDS:
            dedup_dict[lang_field] = linejson[lang_field]
        dedup_bytes = json.dumps(dedup_dict, ensure_ascii=False, sort_keys=True).encode('utf-8')
        # digest = hashlib.sha256(dedup_str).hexdigest() + hashlib.md5(dedup_str).hexdigest() # 选一个快又不那么容易冲突的办法就行
        # digest = hashlib.sha256(dedup_str).hexdigest()
        digest = hashlib.md5(dedup_bytes).digest() + (len(dedup_bytes) % 256).to_bytes(1, signed=False)
        _prvlen = len(dedup_str_set)
        dedup_str_set.add(digest)
        _afterlen = len(dedup_str_set)
        if _afterlen == _prvlen:
            if args.verbose:
                print('【文件级去重】与其它段落完全一致的段落:',dedup_bytes)
            continue
        # filelines = filename2lines.setdefault(linejsonfilename, [])
        # filelines.append(lineidx) # 记有效行的下标
        filename2linecount[linejsonfilename] += 1
        valid_line_idx_set.add((str(file_path),lineidx))

        #######文件级去重#######
        # 计算【去重段落数】、【低质量段落数】，填写【是否重复】
        # low_quality_count = filename2low_quality_count.setdefault(linejson['文件名'], 0)
        zh_text_set: set = filename2zh_text_digest.setdefault(linejsonfilename, set())
        zh_text: str = linejson["zh_text"]
        en_text: str = linejson["en_text"]
        if not zh_text or not en_text:
            filename2low_quality_count[linejsonfilename] += 1
        # _prvlen = len(zh_text_set)
        dedup_bytes = zh_text.encode("utf-8")
        digest = hashlib.md5(dedup_bytes).digest() + (len(dedup_bytes) % 256).to_bytes(1, signed=False) # 内存瓶颈
        zh_text_set.add(digest)
        # _afterlen = len(zh_text_set)
    for filename, zh_text_set in filename2zh_text_digest.items():
        filename2zh_text_dedup_count[filename] = len(zh_text_set)

filename2linecounter = Counter()
bio = BytesIO()
out_file_id = 1

def get_next_out_file_path(parent_dir: Path, file_path: Path):
    if out_file_id == 1:
        if args.all_directory_mode:
            next_out_file_path = parent_dir / "jsonl_reworked" / (args.all_directory_mode + '.jsonl')
        else:
            next_out_file_path = parent_dir / "jsonl_reworked" / file_path.name
    else:
        if args.all_directory_mode:
            next_out_file_path = parent_dir / "jsonl_reworked" / f"{args.all_directory_mode}-{out_file_id}.jsonl"
        else:
            filename_without_ext, file_ext_name = file_path.name.rsplit('.', 1)
            next_out_file_path = parent_dir / "jsonl_reworked" / f"{filename_without_ext}-{out_file_id}.{file_ext_name}"
    return next_out_file_path

def out_file(file_path: Path):
    out_file_dir = file_path.parent / "jsonl_reworked"
    global is_first
    if is_first:
        if os.path.exists(out_file_dir):
            print(f"请确保{out_file_dir}目录为空，否则其内容可能会被覆盖。如不希望请直接结束本程序。")
            if input("请输入Y以确认继续进行:") != 'Y':
                print("程序退出...")
                exit(0)
        else:
            os.makedirs(out_file_dir)
        is_first = False
    del out_file_dir
    global out_file_id
    for lineidx, linejson in enumerate(gen_new_style_line(file_path, True)):
        if (str(file_path), lineidx) not in valid_line_idx_set:
            continue
        linejsonfilename = linejson['文件名']
        filename2linecounter[linejsonfilename] += 1
        dedup_bytes = linejson["zh_text"].encode("utf-8")
        zhmd5 = hashlib.md5(dedup_bytes)
        digest = zhmd5.digest() + (len(dedup_bytes) % 256).to_bytes(1, signed=False)
        zh_text_set = filename2zh_text_digest[linejsonfilename]
        _prvlen = len(zh_text_set)
        zh_text_set.discard(digest)
        _afterlen = len(zh_text_set)
        linejson['是否待查文件'] = False # 平行语料组固定将此字段给False
        linejson['是否重复文件'] = False # 平行语料组固定将此字段给False
        linejson['是否跨文件重复'] = False # 平行语料组固定将此字段给False

        linejson['是否重复'] = _afterlen == _prvlen
        linejson['段落数'] = filename2linecount[linejsonfilename]
        linejson['去重段落数'] = filename2linecount[linejsonfilename] - filename2zh_text_dedup_count[linejsonfilename] # 经核实，此字段统计的是“重复了的段落”的个数
        linejson['低质量段落数'] = filename2low_quality_count[linejsonfilename]
        linejson['行号'] = filename2linecounter[linejsonfilename]
        linejson['zh_text_md5'] = zhmd5.hexdigest()
        outjsonbytes = (json.dumps(linejson, ensure_ascii=False, sort_keys=True) + '\n').encode('utf-8') # 这个是LF格式的换行
        if bio.tell() + len(outjsonbytes) > args.bytes_limit:
            next_out_file_path = get_next_out_file_path(file_path.parent, file_path)
            with open(next_out_file_path, "wb") as fo:
                print("out file:",next_out_file_path)
                fo.write(bio.getbuffer().tobytes())
                # bio.seek(0)
                # bio.readinto(fo)
                bio.seek(0)
                bio.truncate()
                out_file_id += 1
        bio.write(outjsonbytes)

if __name__ == "__main__":
    if args.directory:
        if not args.all_directory_mode:
            for filename in os.listdir(args.directory):
                if filename.endswith('.jsonl'):
                    print('[directory] filename:',filename)
                    file_path = Path(os.path.join(args.directory, filename))
                    process_file(file_path)
                    out_file(file_path)
                    first_warn_unk_key.clear()
                    first_warn_other_texts_key_check.clear()
                    filename2zh_text_digest.clear()
                    filename2low_quality_count.clear()
                    filename2linecount.clear()
                    valid_line_idx_set.clear()
                    filename2zh_text_dedup_count.clear()
                    filename2linedigest.clear()
                    filename2linecounter.clear()
                    if bio.tell() > 0:
                        next_out_file_path = get_next_out_file_path(file_path.parent, file_path)
                        print("out file:",next_out_file_path)
                        with open(next_out_file_path, "wb") as fo:
                            fo.write(bio.getbuffer().tobytes())
                    bio.seek(0)
                    bio.truncate()
                    out_file_id = 1
        else:
            cachepath = Path(os.path.join(args.directory, "stat.pkl"))
            if cachepath.exists():
                with open(cachepath, "rb") as f:
                    print("Load cache file:",cachepath)
                    first_warn_unk_key,first_warn_other_texts_key_check,filename2zh_text_digest,filename2low_quality_count,filename2linecount,valid_line_idx_set,filename2zh_text_dedup_count,filename2linedigest = pickle.load(f)
            else:
                for filename in os.listdir(args.directory):
                    if filename.endswith('.jsonl'):
                        print('[reading directory] filename:',filename)
                        process_file(Path(os.path.join(args.directory, filename)))
                with open(cachepath, "wb") as f:
                    pickle.dump(
                        (
                            first_warn_unk_key,
                            first_warn_other_texts_key_check,
                            filename2zh_text_digest,
                            filename2low_quality_count,
                            filename2linecount,
                            valid_line_idx_set,
                            filename2zh_text_dedup_count,
                            filename2linedigest,
                        ), f, pickle.HIGHEST_PROTOCOL
                    )
                    print("Write cache file:",cachepath)
            for filename in os.listdir(args.directory):
                if filename.endswith('.jsonl'):
                    print('[output] filename:',filename)
                    out_file(Path(os.path.join(args.directory, filename)))
            if bio.tell() > 0:
                next_out_file_path = get_next_out_file_path(Path(args.directory), None)
                print("out file:",next_out_file_path)
                with open(next_out_file_path, "wb") as fo:
                    fo.write(bio.getbuffer().tobytes())
            bio.seek(0)
            bio.truncate()

    elif args.input:
        print('[single file] filename:',args.input)
        input_path = Path(args.input)
        process_file(input_path)
        out_file(input_path)
        if bio.tell() > 0:
            next_out_file_path = get_next_out_file_path(input_path.parent, input_path)
            print("out file:",next_out_file_path)
            with open(next_out_file_path, "wb") as fo:
                fo.write(bio.getbuffer().tobytes())
    else:
        print("请提供一个目录或输入文件路径。")
        exit(0)

    # input("处理完毕，回车关闭")
