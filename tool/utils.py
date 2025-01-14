import gzip
import os
from datetime import datetime
from os.path import join as join_path
from os.path import exists as file_exists
from os import makedirs as create_dirs


import aiofiles
import dateparser
import fitz  # PyMuPDF

'''
验证pdf是否可用
'''
async def async_pdf_valid(file_path):
    if os.path.exists(file_path):
        try:
            async with aiofiles.open(file_path, 'rb') as file:
                content = await file.read()
                doc = fitz.open("pdf", content)
                doc.close()
                return True
        except Exception as e:
            print('PDF文件损坏')
            return False
    else:
        print('PDF文件不存在')
        return False


'''
读取gzip的压缩文件
'''
def read_gzip_file(file_path):
    with gzip.open(file_path, 'rb') as f:
        content = f.read()
    return content

'''
日期格式换
'''
def date_conversion(date):
    config_dict = {'PREFER_DAY_OF_MONTH': 'first', 'RELATIVE_BASE': datetime(1900, 1, 1)}
    parse_date = dateparser.parse(date, settings=config_dict)
    need_date = parse_date.strftime("%Y-%m-%d")
    return need_date


def folder_judge(folder_path):
    if folder_path is not None and folder_path != '' and not file_exists(folder_path):
        create_dirs(folder_path)

'''
保存数据为gzip压缩文件
'''
def write_gzip_file(folder, file_name, content):
    folder_judge(folder)
    file_path = join_path(folder, file_name)
    print('write file: ({})'.format(file_path))
    with gzip.open(file_path, 'wb') as f:
        f.write(content)