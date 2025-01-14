from requests import Session
from requests.sessions import CaseInsensitiveDict


class Http(Session):
    def __init__(self):
        Session.__init__(self)
        self.headers = CaseInsensitiveDict({
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        })

    def set_proxy(self, url):
        self.proxies = {'https': url, 'http': url}


http = Http()
