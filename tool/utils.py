import os
import aiofiles
import fitz  # PyMuPDF

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

