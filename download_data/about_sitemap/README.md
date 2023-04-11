# Install the requirements
```bash
pip install -r requirements.txt
```

# download_pdf_by_sitemap_to_loacl.py script run guide
```bash

python download_pdf_by_sitemap_to_loacl.py --worker_thread=1 --file_save_path="./download_pdf" --erroe_file_save_path="./error.txt" --start=111000 --end=-1

在以上指令中：
    - worker_thread 并行的核心数量，不建议设置过大（可能会触发网站对于运行者的限流/熔断）
        - 不可以设置负数
        - 默认1
        - 0：根据电脑核数匹配并行数量（8核电脑=8个进程）
        - 1：单核运行
        - 其他：等于进程数量
    - file_save_path 文件保存的文件夹路径 
        - 默认"./download_pdf"
    - erroe_file_save_path 错误文件保存的路径及其文件名（尽量文件后缀为txt）\
        - 默认"./error.txt"
    - start 开始下载的索引位置
        - 必须大于0
        - 默认0（从头开始）
    - end 结束下载的索引位置
        - -1: 到最后
        - 默认-1
    0 < start < end < ~210000
```


```bash

python download_pdf_by_sitemap_to_loacl.py --downloaded_pdf_path="./download_pdf" --pdf_text_file_path="./pdf_text_file"

在以上指令中：
    - downloaded_pdf_path 由download_pdf_by_sitemap_to_loacl脚本跑出来的并保存pdf的文件夹
        - 默认"./download_pdf"
    - pdf_text_file_path 保存pdf_text的文件夹
        - 默认"./pdf_text_file"
      
```