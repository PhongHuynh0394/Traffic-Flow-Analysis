import requests
from .proxy_provider import WebshareProvider, GimmeproxyProvider, ProxyProvider
from typing import List, Dict, Optional, Union
import os
import json
import random
import logging
from urllib.parse import urlparse
        

class ProxyManager:
    _CACHE_FILE = ".proxy_cache.json"

    def __init__(self, webshare_token: Union[str, List[str]], cachefolder="./cache_proxy"):

        self._cache_path = os.path.join(cachefolder, self._CACHE_FILE)
        self.blacklist = []
        self._webshare_token = webshare_token

        # init the proxy list
        self.proxy_provider = self.init_provider_list()
        self._last_proxy = {}

        # Update proxy list and write cache
        if os.path.exists(self._cache_path):
            self.proxy_list = self.get_proxy_list()
        else:
            logging.info("There is no cached file, fetch proxy list from API to {}".format(self._cache_path))
            self.update()


    def init_provider_list(self) -> dict[str, ProxyProvider]:

        provider_list: list[ProxyProvider] = []
        if self._webshare_token is not None:
            if isinstance(self._webshare_token, str):
                provider_list.append(WebshareProvider(token=self._webshare_token)) 
            else:
                for token in self._webshare_token:
                    provider_list.append(WebshareProvider(token=token))

        # if not provider_list:
        #     provider_list.append(GimmeproxyProvider())

        return provider_list
    

    def update(self):
        self._cache()
        self.proxy_list = self.get_proxy_list() # update memory proxy list


    def add_blacklist(self, proxy: Dict[str, str]):
        try:
            index = self.proxy_list.index(proxy)
        except Exception:
            logging.warning("Proxy not found in current proxy list")
            return

        self.proxy_list.pop(index)
        self.blacklist.extend(proxy['http'])
    

    def _parse_host(self, url):
        if '@' in url:
            path = url.split('@')[-1]
            # subnet = path.split['.'][2]
            host = path.split(":")[0]
        else:
            host = urlparse(url).hostname
            # subnet = host.split('.')[2]
        
        return host

    
    def _get_asn(self, url):
        host = self._parse_host(url)
        response = requests.get(f"https://ipinfo.io/{host}/json")
        data = response.json()
        asn = data.get("org")
        return 'Unknown' if asn is None else asn.split()[0]
    

    def _rotate_by_asn(self):
        random_proxy = random.choice(self.proxy_list)

        # init rotate
        if not self._last_proxy:
            return random_proxy
        
        last_asn = self._get_asn(self._last_proxy['http'])
        while True:
            current_asn = self._get_asn(random_proxy['http'])

            if current_asn != last_asn:
                break

            # Continue rotating
            random_proxy = random.choice(self.proxy_list)

        return random_proxy


    def rotate(self, by_asn=True):

        if not os.path.exists(self._cache_path):
            logging.info("There is no cached file, fetch from API and update memory")
            self.update()

        if by_asn:
            proxy = self._rotate_by_asn()
        else:
            proxy = random.choice(self.proxy_list)
        
        self._last_proxy = proxy
        return proxy


    def get_proxy_list(self):
        with open(self._cache_path, 'r') as f:
            proxies = json.load(f)
        
        return proxies

    
    def _cache(self):
        logging.info("Fetching list...")
        proxy_list = []
        for provier in self.proxy_provider:
            proxy_list.extend(provier.get_list())
        
        os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)

        with open(self._cache_path, 'w') as f:
            json.dump(proxy_list, f)
    

    def clear_cache(self):
        logging.info("Clean cache")
        if os.path.exists(self._cache_path):
            os.remove(self._cache_path)
        self.proxy_list = []