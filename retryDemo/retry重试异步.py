import asyncio
from functools import wraps
import logging


def retry(max_retries=1, delay=0, default_value=None, raise_exception=False):
    """
    :param max_retries: 最大重试次数
    :param delay: 重试间隔秒数
    :param default_value: 超过最大重试次数返回的默认值
    :param raise_exception: 是否抛出程序异常,默认False
    :return:
    """
    def decorator(function):
        num_tuple = (max_retries, delay)

        @wraps(function)
        async def wrapper(*args, **kwargs):
            count_num, sleep_num = num_tuple
            retries = 0
            while retries < count_num:
                try:
                    return await function(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    logging.error(f"Retry {retries}/{max_retries} due to exception: {str(e)}")
                    await asyncio.sleep(sleep_num)
            if raise_exception:
                raise RuntimeError("超过最大重试次数!")
            else:
                return default_value

        return wrapper

    return decorator
