import argparse
import datasets
import json
import time
import random
import os
from filelock import FileLock
import wandb
from pathlib import Path
import sys
import logging


logging.basicConfig(level=logging.INFO)


# 防止用户在其他位置调用此脚本，从而找不到库的情况
current_dir = os.path.dirname(os.path.abspath(__file__))
alignment_path = os.path.join(current_dir, '..')
sys.path.append(alignment_path)


import alignment.utils as utils
from alignment.batch_detector import GPTBatchDetector


# 所有的缓存与相关文件同意放到标本脚本目录处
RECORD_INDEX_MAP_FILE_LOACATION = f"{os.path.dirname(os.path.abspath(__file__))}/record_index_map.json"
LOCK_FILE_LOCATION = f"{os.path.dirname(os.path.abspath(__file__))}/record_index_map.json.lock"
CACHE_DIR = f"{os.path.dirname(os.path.abspath(__file__))}/gpt_cache"

Path(CACHE_DIR).mkdir(exist_ok=True)

class SingleFileSegmentbuilder:

    def __init__(self, api_key=None):
        self.api_key = api_key
        self.dataset_row = self.get_dataset_row()
        logging.info(f"{self.record} start")


    def done_in_json_settings_file(self):
        """
        In the json file set the current file is done

        """

        # lock 可初步理解成自旋锁
        with FileLock(LOCK_FILE_LOCATION):
            with open(RECORD_INDEX_MAP_FILE_LOACATION, "r+") as f:
                record_index_map = json.load(f)
                record_index_map[self.record]["completed"] = True # record已完成
                record_index_map[self.record]["processing"] = False # record处理完毕，所以为Flase

                f.seek(0) # 将文件指针移动到文件开头
                f.truncate()  # 清空文件内容

                json.dump(record_index_map, f) # 将更新后的record索引映射写回文件
                f.flush() # 将文件内容刷新到磁盘

    def get_dataset_row(self):
        """
        Get unused rows in datasets

        Returns: 
            record: file record number
            dataset_row: dict('zh', 'en', 'fr', 'es', 'ru', 'record')

        """

        # lock 可初步理解成自旋锁
        with FileLock(LOCK_FILE_LOCATION):
            with open(RECORD_INDEX_MAP_FILE_LOACATION, "r+") as f:
                record_index_map = json.load(f)
                
                # 准备的数据集索引
                prepare_dataset_index = None 

                # 寻找下一个可用的record，并找到对应dataset的index
                for record in record_index_map:
                    completed = record_index_map[record]["completed"] # record是否已完成
                    processing = record_index_map[record]["processing"] # record是否正在处理

                    # 如果record既未完成也未在处理中
                    if not (completed or processing):
                        prepare_dataset_index = record_index_map[record]["index"]
                        record_index_map[record]["processing"] = True
                        self.record = record
                        break
                
                f.seek(0) # 将文件指针移动到文件开头
                f.truncate() # 清空文件内容

                json.dump(record_index_map, f) # 将更新后的record索引映射写回文件
                f.flush() # 将文件内容刷新到磁盘

                if not prepare_dataset_index:
                    raise Exception("Could not find next available file")

                dataset = datasets.load_dataset("bot-yaya/un_pdf_random10000_preprocessed", verification_mode="no_checks")["train"]
                # 返回准备的数据集索引对应的数据
                return dataset[prepare_dataset_index]



    def start(self):
        detector = GPTBatchDetector('gpt-remote', CACHE_DIR, api_key=self.api_key)
        lines = self.dataset_row['en'].splitlines()

        predicted = detector.detect(lines, record_id=self.record)
        self.predicted = predicted
        return self.predicted


    def post_process(self):
        pass


    def batch_post_process(self):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--api_key', type=str, default=False, help='openai api key')
    parser.add_argument('--test_mode', type=int, default=0, help='是否测试此脚本 0/1')

    args = parser.parse_args()
    api_key = args.api_key
    test_mode = args.test_mode

    # 测试
    if test_mode:
        singleFileSegmentbuilder = SingleFileSegmentbuilder(api_key=api_key)
        record = singleFileSegmentbuilder.record
        print(f"{record} start")
        print(f"{args.api_key} api_key")
        singleFileSegmentbuilder.done_in_json_settings_file()
        print(f"{record} success")
        sys.exit(0)

    if not api_key:
        raise ValueError("params --key must input")

    singleFileSegmentbuilder = SingleFileSegmentbuilder(api_key=api_key)
    record = singleFileSegmentbuilder.record

    #  使wandn不会把openai key记录到wandb-metadata.json中
    sys.argv = [arg for arg in sys.argv if not arg.startswith("--key")]

    wandb.init(project="single_file_segment_builder", name=f"GPTBatchDetector-{record}")
    run = wandb.run

    artifact = wandb.Artifact(
        name="single_file_segment_builder",
        type="dataset",
        description="JSON files only containing predictions and record_id",
        metadata=dict(record=record))


    predicted = singleFileSegmentbuilder.start()
    singleFileSegmentbuilder.post_process()
    singleFileSegmentbuilder.done_in_json_settings_file()

    logging.info(f"{record} success")

    with artifact.new_file(f"{record}-is-hard-linebreak.json", mode="w") as f:
        json.dump(predicted, f)

    run.log_artifact(artifact)

    wandb.finish()
    