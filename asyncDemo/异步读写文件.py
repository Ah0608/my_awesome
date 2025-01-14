import logging
from sys import stdout

import aiofiles
from os.path import exists as file_exists
from os.path import join as path_join
from os import makedirs as create_dirs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=stdout)
async def async_read_file(filename):
    if not file_exists(filename):
        raise FileNotFoundError(f"{filename} does not exist.")

    async with aiofiles.open(filename, 'rb') as file:
        content = await file.read()
        return content

async def async_write_file(folder, filename, content):
    if not file_exists(folder):
        create_dirs(folder)
    file_path = path_join(folder, filename)
    logging.info('write file: ({})'.format(file_path))
    async with aiofiles.open(file_path, 'wb') as file:
        await file.write(content)
        return file_path
