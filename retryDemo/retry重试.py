import logging
from time import sleep
from functools import wraps


def retry(max_retries=1, delay=0, default_value=None, raise_exception=False):
    """
    :param max_retries: 最大重试次数
    :param delay: 重试间隔秒数
    :param default_value: 超过最大重试次数返回的默认值
    :param raise_exception: 是否抛出程序异常,默认False
    :return:
    """
    def decorate(function):
        num_tuple = (max_retries, delay)

        @wraps(function)
        def wrapper(*args, **kwargs):
            count_num, sleep_num = num_tuple
            retries = 0
            while retries < count_num:
                try:
                    return function(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    logging.error(f"Retry {retries}/{max_retries} due to exception: {str(e)}")
                    sleep(sleep_num)
            if raise_exception:
                raise RuntimeError("超过最大重试次数!")
            else:
                return default_value

        return wrapper

    return decorate
