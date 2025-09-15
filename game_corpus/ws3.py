# 女巫之泉3
from base_corpus import BaseCorpus
import os

lang_dic = {
    'Kr': 'ko_text',
    'En': 'en_text',
    'Fr': 'fr_text',
    'Ja': 'ja_text',
    'Tw': 'cht_text',
    'Cn': 'zh_text'
}


class WS3Corpus(BaseCorpus):
    def load_contents(self):
        for file in os.listdir(self.path):
            with open(os.path.join(self.path, file), 'r', encoding='utf-8') as fp:
                content = fp.read()
                lines = content.split('\n')
                for i in range(1, len(lines)):
                    if not lines[i].strip():
                        continue
                    fields = lines[i].split('|')
                    fields = [f.strip() for f in fields]
                    key = '_'.join([file] + fields[:-6])
                    if key not in self.key_content:
                        self.key_content[key] = {lang_dic[list(lang_dic.keys())[j]]: fields[-6:][j] for j in range(6)}


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--game_path', type=str, required=True, help='the corpus path of the game')

    args = parser.parse_args()
    game_path = args.game_path

    corpus = WS3Corpus(game_path)
    corpus.init_lang_dic(lang_dic)
    corpus.load_contents()
    corpus.export_corpus_new()
