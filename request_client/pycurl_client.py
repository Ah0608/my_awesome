from urllib.parse import urlencode
from queue import Queue
import pycurl
import asyncio
import certifi
from io import BytesIO

# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class Response(object):
    headers = {}
    content = b''
    http_code = 200
    effective_url = ''


class Request(object):
    def __init__(self):
        self.handle = pycurl.Curl()
        self.set_option(pycurl.ENCODING, '')
        self.set_option(pycurl.CAINFO, certifi.where())
        self.headers = {}

        def header_function(header_line):
            header_line = header_line.decode()
            if ':' not in header_line:
                return
            name, value = header_line.split(':', 1)
            name = name.strip()
            value = value.strip()
            name = name.lower()
            self.headers[name] = value

        self.set_option(pycurl.HEADERFUNCTION, header_function)

    def set_timeout(self, timeout):
        "Set timeout for a retrieving an object"
        self.set_option(pycurl.TIMEOUT, timeout)
        self.set_option(pycurl.LOW_SPEED_TIME, timeout)
        self.set_option(pycurl.LOW_SPEED_LIMIT, 1)

    def set_redirect(self, max_redirect=5):
        self.set_option(pycurl.FOLLOWLOCATION, 1)
        self.set_option(pycurl.MAXREDIRS, max_redirect)

    def set_option(self, *args):
        "Set an option on the retrieval."
        self.handle.setopt(*args)

    def set_verbosity(self, level):
        "Set verbosity to 1 to see transactions."
        self.set_option(pycurl.VERBOSE, level)

    def set_proxy(self, proxy_url):
        self.set_option(pycurl.PROXY, proxy_url)

    def impersonate(self, target='chrome110', default_headers=1):
        self.handle.impersonate(target, default_headers)

    def set_cookie_file(self, file_path):
        self.set_option(pycurl.COOKIEFILE, file_path)
        self.set_option(pycurl.COOKIEJAR, file_path)

    def __request(self, url, header):
        "Perform the pending request."
        if header is not None:
            headers_list = [f'{key}: {value}' for key, value in header.items()]
            self.set_option(pycurl.HTTPHEADER, headers_list)
        self.set_option(pycurl.URL, url)
        self.headers = {}
        response = Response()
        response.headers = self.headers
        response.content = self.handle.perform_rb()
        response.http_code = self.get_info(pycurl.RESPONSE_CODE)
        response.effective_url = self.get_info(pycurl.EFFECTIVE_URL)
        return response

    def get(self, url="", params=None, headers=None):
        "Ship a GET request for a specified URL, capture the response."
        if params:
            url += "?" + urlencode(params)
        self.set_option(pycurl.HTTPGET, 1)
        return self.__request(url, headers)

    def head(self, url="", params=None, headers=None):
        "Ship a HEAD request for a specified URL, capture the response."
        if params:
            url += "?" + urlencode(params)
        self.set_option(pycurl.NOBODY, 1)
        return self.__request(url, headers)

    def post(self, url, data, headers=None):
        "Ship a POST request to a specified CGI, capture the response."
        self.set_option(pycurl.POST, 1)
        self.set_option(pycurl.POSTFIELDS, data)
        return self.__request(url, headers)

    def get_info(self, *args):
        "Get information about retrieval."
        return self.handle.getinfo(*args)

    def close(self):
        "Close a session, freeing resources."
        if self.handle:
            self.handle.close()
        self.handle = None
        self.headers = ""

    def __del__(self):
        self.close()


class RequestThread(object):
    def __init__(self, max_clients=5, target='chrome110', default_headers=1, enable_cookie=False, cookie_path=''):
        self.curl_queue = Queue()
        self.share = pycurl.CurlShare()
        self.share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
        self.share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
        self.share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_SSL_SESSION)
        self.follow_redirects = True
        self.max_redirects = 5
        self.verify = True
        self.proxy_url = None
        self.timeout = None
        self.ca_path = certifi.where()
        for _ in range(max_clients):
            curl = self.create_curl(enable_cookie=enable_cookie, cookie_path=cookie_path, target=target,
                                    default_headers=default_headers)
            self.curl_queue.put(curl)

    def create_curl(self, enable_cookie=False, cookie_path='', target='chrome110', default_headers=1):
        curl = pycurl.Curl()
        curl.setopt(pycurl.NOSIGNAL, 1)
        curl.setopt(pycurl.ENCODING, '')
        curl.setopt(pycurl.SHARE, self.share)
        curl.setopt(pycurl.CAINFO, self.ca_path)
        curl.impersonate(target, default_headers)
        if enable_cookie:
            curl.setopt(pycurl.COOKIEFILE, cookie_path)
        return curl

    def close(self):
        while True:
            try:
                curl = self.curl_queue.get(timeout=1)
                curl.close()
            except:
                break
        self.share.close()

    def _curl_setup_request(self, curl, url, response_headers, method, headers=None, body=None, timeout=None,
                            follow_redirects=None, max_redirects=None, proxy_url=None, verify=None):
        curl.setopt(pycurl.URL, url)
        if headers is not None:
            if "Expect" not in headers:
                headers["Expect"] = ""
            if "Pragma" not in headers:
                headers["Pragma"] = ""
            curl.setopt(
                pycurl.HTTPHEADER,
                [
                    b"%s: %s"
                    % (k.encode("ASCII"), v.encode("ISO8859-1"))
                    for k, v in headers.items()
                ],
            )
        else:
            curl.setopt(pycurl.HTTPHEADER, None)

        def header_function(header_line):
            header_line = header_line.decode('iso-8859-1')
            if ':' not in header_line:
                return
            name, value = header_line.split(':', 1)
            name = name.strip()
            value = value.strip()
            name = name.lower()
            response_headers[name] = value

        curl.setopt(pycurl.HEADERFUNCTION, header_function)

        if follow_redirects is False or (follow_redirects is None and self.follow_redirects is False):
            curl.setopt(pycurl.FOLLOWLOCATION, False)
        else:
            curl.setopt(pycurl.FOLLOWLOCATION, True)
            if max_redirects is not None:
                curl.setopt(pycurl.MAXREDIRS, max_redirects)
            else:
                curl.setopt(pycurl.MAXREDIRS, self.max_redirects)

        if timeout or self.timeout:
            if timeout is not None:
                curl.setopt(pycurl.CONNECTTIMEOUT, timeout)
            else:
                curl.setopt(pycurl.CONNECTTIMEOUT, self.timeout)
        else:
            curl.setopt(pycurl.LOW_SPEED_TIME, 300)

        curl.setopt(pycurl.CONNECTTIMEOUT, 30)
        curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)

        if proxy_url or self.proxy_url:
            if proxy_url is not None:
                curl.setopt(pycurl.PROXY, proxy_url)
            else:
                curl.setopt(pycurl.PROXY, self.proxy_url)
        else:
            curl.setopt(pycurl.PROXY, None)

        if verify is False or (verify is None and self.verify is False):
            curl.setopt(pycurl.SSL_VERIFYPEER, 0)
            curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        else:
            curl.setopt(pycurl.SSL_VERIFYPEER, 1)
            curl.setopt(pycurl.SSL_VERIFYHOST, 2)

        curl_options = {
            "GET": pycurl.HTTPGET,
            "POST": pycurl.POST,
            "PUT": pycurl.UPLOAD,
            "HEAD": pycurl.NOBODY,
        }
        custom_methods = {"DELETE", "OPTIONS", "PATCH"}
        if method in curl_options:
            curl.unsetopt(pycurl.CUSTOMREQUEST)
            curl.setopt(curl_options[method], True)
        elif method in custom_methods:
            curl.setopt(pycurl.CUSTOMREQUEST, method)
        else:
            raise KeyError("unknown method " + method)

        body_expected = method in ("POST", "PATCH", "PUT")
        body_present = body is not None

        if body_expected or body_present:
            if method == "GET":
                raise ValueError("Body must be None for GET request")
            request_buffer = BytesIO((body or "").encode('utf-8'))

            def ioctl(cmd: int) -> None:
                if cmd == curl.IOCMD_RESTARTREAD:  # type: ignore
                    request_buffer.seek(0)

            curl.setopt(pycurl.READFUNCTION, request_buffer.read)
            curl.setopt(pycurl.IOCTLFUNCTION, ioctl)
            if method == "POST":
                curl.setopt(pycurl.POSTFIELDSIZE, len(body or ""))
            else:
                curl.setopt(pycurl.UPLOAD, True)
                curl.setopt(pycurl.INFILESIZE, len(body or ""))

    def _finish(self, curl, response):
        try:
            response.content = curl.perform_rb()
            response.http_code = curl.getinfo(pycurl.RESPONSE_CODE)
            response.effective_url = curl.getinfo(pycurl.EFFECTIVE_URL)
        finally:
            self.curl_queue.put(curl)
        return response

    def get(self, url, **kwargs):
        curl = self.curl_queue.get()
        response = Response()
        self._curl_setup_request(curl, url, response.headers, "GET", **kwargs)
        return self._finish(curl, response)

    def post(self, url, **kwargs):
        curl = self.curl_queue.get()
        response = Response()
        self._curl_setup_request(curl, url, response.headers, "POST", **kwargs)
        return self._finish(curl, response)

    def put(self, url, **kwargs):
        curl = self.curl_queue.get()
        response = Response()
        self._curl_setup_request(curl, url, response.headers, "PUT", **kwargs)
        return self._finish(curl, response)

    def head(self, url, **kwargs):
        curl = self.curl_queue.get()
        response = Response()
        self._curl_setup_request(curl, url, response.headers, "HEAD", **kwargs)
        return self._finish(curl, response)

    def options(self, url, **kwargs):
        curl = self.curl_queue.get()
        response = Response()
        self._curl_setup_request(curl, url, response.headers, "OPTIONS", **kwargs)
        return self._finish(curl, response)

    def patch(self, url, **kwargs):
        curl = self.curl_queue.get()
        response = Response()
        self._curl_setup_request(curl, url, response.headers, "PATCH", **kwargs)
        return self._finish(curl, response)

    def delete(self, url, **kwargs):
        curl = self.curl_queue.get()
        response = Response()
        self._curl_setup_request(curl, url, response.headers, "DELETE", **kwargs)
        return self._finish(curl, response)


class RequestAsync(object):
    @classmethod
    async def create(cls, max_clients=5, target='chrome110', default_headers=1, enable_cookie=False, cookie_path=''):
        self = RequestAsync()
        self._share = pycurl.CurlShare()
        self._share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
        self._share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
        self._share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_SSL_SESSION)
        self._curls = [self._create_curl(enable_cookie=enable_cookie, cookie_path=cookie_path, target=target,
                                         default_headers=default_headers) for i in range(max_clients)]
        self._free_queue = asyncio.Queue()
        for _ in self._curls:
            await self._free_queue.put(_)
        return self

    def __init__(self):
        self._multi = pycurl.CurlMulti()
        self._multi.setopt(pycurl.M_SOCKETFUNCTION, self._socket_callback)
        self._multi.setopt(pycurl.M_TIMERFUNCTION, self._timer_callback)
        self.follow_redirects = True
        self.max_redirects = 5
        self.verify = True
        self.proxy_url = None
        self.timeout = None
        self.ca_path = certifi.where()
        self._timer = None

        self._transfers = {}

        self._fds = set()

    def _create_curl(self, enable_cookie=False, cookie_path='', target='chrome110', default_headers=1):
        curl = pycurl.Curl()
        curl.setopt(pycurl.NOSIGNAL, 1)
        curl.setopt(pycurl.ENCODING, '')
        curl.setopt(pycurl.SHARE, self._share)
        curl.setopt(pycurl.CAINFO, self.ca_path)
        curl.impersonate(target, default_headers)
        if enable_cookie:
            curl.setopt(pycurl.COOKIEFILE, cookie_path)
        return curl

    def _socket_callback(self, ev_bitmask, sock_fd, multi, data):
        loop = asyncio.get_running_loop()

        if sock_fd in self._fds:
            loop.remove_reader(sock_fd)
            loop.remove_writer(sock_fd)

        if ev_bitmask == pycurl.POLL_IN or ev_bitmask == pycurl.POLL_INOUT:
            loop.add_reader(sock_fd, self._socket_action, sock_fd, pycurl.CSELECT_IN)
            self._fds.add(sock_fd)

        if ev_bitmask == pycurl.POLL_OUT or ev_bitmask == pycurl.POLL_INOUT:
            loop.add_writer(sock_fd, self._socket_action, sock_fd, pycurl.CSELECT_OUT)
            self._fds.add(sock_fd)

        if ev_bitmask == pycurl.POLL_REMOVE:
            self._fds.remove(sock_fd)

    def _timer_callback(self, timeout_ms):
        if self._timer:
            self._timer.cancel()
        if timeout_ms == -1:
            self._timer = None
        else:
            loop = asyncio.get_running_loop()
            self._timer = loop.call_later(timeout_ms / 1000, self._multi.socket_action, pycurl.SOCKET_TIMEOUT, 0)

    def _socket_action(self, sock_fd, ev_bitmask):
        status, handle_count = self._multi.socket_action(sock_fd, ev_bitmask)
        status, handle_count = self._multi.socket_all()

        if handle_count != len(self._transfers):
            self._update_transfers()

    def _update_transfers(self):
        more_info, succ_handles, fail_handles = self._multi.info_read()
        for handle in succ_handles:
            self._remove_handle(handle, result=None)

        for handle, errno, errmsg in fail_handles:
            self._remove_handle(handle, exception=pycurl.error(errno, errmsg))

        if more_info:
            self._update_transfers()

    def _add_handle(self, handle: pycurl.Curl):
        self._multi.add_handle(handle)

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._transfers[handle] = future
        return future

    def _remove_handle(self, handle: pycurl.Curl, result=None, exception=None, cancel=False):
        self._multi.remove_handle(handle)

        future = self._transfers.pop(handle)
        if cancel:
            future.cancel()
        elif exception:
            future.set_exception(exception)
        else:
            future.set_result(result)

    def _stop(self, handle: pycurl.Curl):
        self._remove_handle(handle, result=None)

    def _cancel(self, handle: pycurl.Curl):
        self._remove_handle(handle, cancel=True)

    def close(self):
        for handle in self._transfers.keys():
            self._stop(handle)
        for handle in self._curls:
            handle.close()
        self._share.close()
        self._multi.close()

    def _curl_setup_request(self, curl, url, response_headers, buffer, method, headers=None, body=None, timeout=None,
                            follow_redirects=None, max_redirects=None, proxy_url=None, verify=None):
        curl.setopt(pycurl.URL, url)
        curl.setopt(pycurl.WRITEDATA, buffer)
        if headers is not None:
            if "Expect" not in headers:
                headers["Expect"] = ""
            if "Pragma" not in headers:
                headers["Pragma"] = ""
            curl.setopt(
                pycurl.HTTPHEADER,
                [
                    b"%s: %s"
                    % (k.encode("ASCII"), v.encode("ISO8859-1"))
                    for k, v in headers.items()
                ],
            )
        else:
            curl.setopt(pycurl.HTTPHEADER, None)

        def header_function(header_line):
            header_line = header_line.decode('iso-8859-1')
            if ':' not in header_line:
                return
            name, value = header_line.split(':', 1)
            name = name.strip()
            value = value.strip()
            name = name.lower()
            response_headers[name] = value

        curl.setopt(pycurl.HEADERFUNCTION, header_function)

        if follow_redirects is False or (follow_redirects is None and self.follow_redirects is False):
            curl.setopt(pycurl.FOLLOWLOCATION, False)
        else:
            curl.setopt(pycurl.FOLLOWLOCATION, True)
            if max_redirects is not None:
                curl.setopt(pycurl.MAXREDIRS, max_redirects)
            else:
                curl.setopt(pycurl.MAXREDIRS, self.max_redirects)

        if timeout or self.timeout:
            if timeout is not None:
                curl.setopt(pycurl.CONNECTTIMEOUT, timeout)
            else:
                curl.setopt(pycurl.CONNECTTIMEOUT, self.timeout)
        else:
            curl.setopt(pycurl.CONNECTTIMEOUT, 300)

        curl.setopt(pycurl.LOW_SPEED_TIME, 30)
        curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)

        if proxy_url or self.proxy_url:
            if proxy_url is not None:
                curl.setopt(pycurl.PROXY, proxy_url)
            else:
                curl.setopt(pycurl.PROXY, self.proxy_url)
        else:
            curl.setopt(pycurl.PROXY, None)

        if verify is False or (verify is None and self.verify is False):
            curl.setopt(pycurl.SSL_VERIFYPEER, 0)
            curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        else:
            curl.setopt(pycurl.SSL_VERIFYPEER, 1)
            curl.setopt(pycurl.SSL_VERIFYHOST, 2)

        curl_options = {
            "GET": pycurl.HTTPGET,
            "POST": pycurl.POST,
            "PUT": pycurl.UPLOAD,
            "HEAD": pycurl.NOBODY,
        }
        custom_methods = {"DELETE", "OPTIONS", "PATCH"}
        if method in curl_options:
            curl.unsetopt(pycurl.CUSTOMREQUEST)
            curl.setopt(curl_options[method], True)
        elif method in custom_methods:
            curl.setopt(pycurl.CUSTOMREQUEST, method)
        else:
            raise KeyError("unknown method " + method)

        body_expected = method in ("POST", "PATCH", "PUT")
        body_present = body is not None

        if body_expected or body_present:
            if method == "GET":
                raise ValueError("Body must be None for GET request")
            request_buffer = BytesIO((body or "").encode('utf-8'))

            def ioctl(cmd: int) -> None:
                if cmd == curl.IOCMD_RESTARTREAD:  # type: ignore
                    request_buffer.seek(0)

            curl.setopt(pycurl.READFUNCTION, request_buffer.read)
            curl.setopt(pycurl.IOCTLFUNCTION, ioctl)
            if method == "POST":
                curl.setopt(pycurl.POSTFIELDSIZE, len(body or ""))
            else:
                curl.setopt(pycurl.UPLOAD, True)
                curl.setopt(pycurl.INFILESIZE, len(body or ""))

    async def _finish(self, curl, response, buffer):
        try:
            await self._add_handle(curl)
            response.content = buffer.getvalue()
            response.http_code = curl.getinfo(pycurl.RESPONSE_CODE)
            response.effective_url = curl.getinfo(pycurl.EFFECTIVE_URL)
        finally:
            await self._free_queue.put(curl)
        return response

    async def get(self, url, **kwargs):
        curl = await self._free_queue.get()
        buffer = BytesIO()
        response = Response()
        self._curl_setup_request(curl, url, response.headers, buffer, "GET", **kwargs)
        return await self._finish(curl, response, buffer)

    async def post(self, url, **kwargs):
        curl = await self._free_queue.get()
        response = Response()
        buffer = BytesIO()
        self._curl_setup_request(curl, url, response.headers, buffer, "POST", **kwargs)
        return await self._finish(curl, response, buffer)

    async def put(self, url, **kwargs):
        curl = await self._free_queue.get()
        response = Response()
        buffer = BytesIO()
        self._curl_setup_request(curl, url, response.headers, buffer, "PUT", **kwargs)
        return await self._finish(curl, response, buffer)

    async def head(self, url, **kwargs):
        curl = await self._free_queue.get()
        response = Response()
        buffer = BytesIO()
        self._curl_setup_request(curl, url, response.headers, buffer, "HEAD", **kwargs)
        return await self._finish(curl, response, buffer)

    async def options(self, url, **kwargs):
        curl = await self._free_queue.get()
        response = Response()
        buffer = BytesIO()
        self._curl_setup_request(curl, url, response.headers, buffer, "OPTIONS", **kwargs)
        return await self._finish(curl, response, buffer)

    async def patch(self, url, **kwargs):
        curl = await self._free_queue.get()
        response = Response()
        buffer = BytesIO()
        self._curl_setup_request(curl, url, response.headers, buffer, "PATCH", **kwargs)
        return await self._finish(curl, response, buffer)

    async def delete(self, url, **kwargs):
        curl = await self._free_queue.get()
        response = Response()
        buffer = BytesIO()
        self._curl_setup_request(curl, url, response.headers, buffer, "DELETE", **kwargs)
        return await self._finish(curl, response, buffer)
