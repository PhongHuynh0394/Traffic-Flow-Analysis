import requests
from bs4 import BeautifulSoup
import json
import random

class UserAgentRotator:
    _COMMON_UA_URL = "https://www.useragents.me/"

    _STATIC_UA_POOL = [
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",

        # Chrome
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_6_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

        #Firefox
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) Gecko/20100101 Firefox/114.0",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:113.0) Gecko/20100101 Firefox/113.0",
    ]
    def __init__(self):
        self.update()


    def _fetch_ua(self):

        r = requests.get(self._COMMON_UA_URL)
        soup = BeautifulSoup(r.text, "html.parser")

        ua_div = soup.find("div", attrs={"id": "most-common-desktop-useragents-json-csv"})
        ua = ua_div.find("textarea", class_="form-control")
        ua_list = json.loads(ua.text.strip())
        ua_weight = [entry["pct"] for entry in ua_list]
        ua_list = [entry["ua"] for entry in ua_list]

        return ua_list, ua_weight
    

    def update(self):
        """
        Fetch COMMON UA URL and load UA list to memory
        """
        self.common_ua_list, self.common_ua_weight = self._fetch_ua()

    
    def rotate(self, static_common=[30, 70]):
        use_static_pool = random.choices([True, False], weights=static_common, k=1)[0]
        if use_static_pool:
            return random.choice(self._STATIC_UA_POOL)
        else:
            return random.choices(self.common_ua_list, weights=self.common_ua_weight, k=1)[0]
