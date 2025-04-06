from abc import abstractmethod, ABC
import os
import requests
import logging
from concurrent.futures import ThreadPoolExecutor

class ProxyProvider(ABC):

    def __init__(self, token: str | None = None):
        self._token = token

    @abstractmethod
    def get_list(self) -> list[dict]:
        raise NotImplementedError(f"{self.__class__.__name__}")


class WebshareProvider(ProxyProvider):
    _PROXY_API_URL = "https://proxy.webshare.io/api/v2/proxy/list/"
    TOKEN_REQUIRED = True

    def __init__(self, name: str = "WEBSHARE", token: str | None = None):
        super().__init__(token)
        self.name = name

    
    def get_list(self) -> list[dict]:
        params = {
            'valid': 'true',
            'mode': 'direct',
            'page': '1',
            'page_size': '25',
        }

        try:
            proxy_list = requests.get(
                self._PROXY_API_URL,
                headers={"Authorization": f"Token {self._token}"},
                params=params
            )
        except Exception as e:
            raise Exception(f"Failed to fetch proxy list: {e}")
        
        proxy_list = proxy_list.json()['results']

        template = 'http://{username}:{password}@{host}:{port}'

        result = [{
            "http": template.format(username=proxy['username'],
                                    password=proxy['password'],
                                    host=proxy['proxy_address'],
                                    port=proxy['port']),
            "https": template.format(username=proxy['username'],
                                    password=proxy['password'],
                                    host=proxy['proxy_address'],
                                    port=proxy['port']),
        } for proxy in proxy_list]

        return result


class GimmeproxyProvider(ProxyProvider):
    _PROXY_API_URL = "https://gimmeproxy.com/api/getProxy"
    TOKEN_REQUIRED = False
    LIST_SIZE = 30
    

    def __init__(self, name: str = "GIMMEPROXY", token: str | None = None):
        self.name = name
    
    def _get_proxy(self) -> str:

        querystring = {"protocol":"http",
                       "supportsHttps":"true",
                    #    "ipPort":"true",
                       "curl":"true",
                       "anonymityLevel":"1",
                       "user-agent":"true",
                       "referer":"true",
                       "get":"true"}
        try:
            response = requests.get(
                self._PROXY_API_URL,
                params=querystring
            )
        except Exception as e:
            logging.warning(f"Failed to fetch proxy: {e}")
            return None

        return response.text

    def get_list(self) -> list[dict]:
        worker = os.cpu_count() * 2
        with ThreadPoolExecutor(max_workers=worker) as executor:
            proxy_set = set(executor.map(lambda _: self._get_proxy(), range(self.LIST_SIZE)))

        result = [{'http': proxy, 'https': proxy} for proxy in proxy_set]
        return result