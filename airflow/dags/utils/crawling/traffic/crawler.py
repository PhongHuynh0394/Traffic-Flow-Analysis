from ..basecrawler import BaseCrawler
import json
import os
import re
import requests

class TrafficCrawler(BaseCrawler):
    _BASE_URL = "https://giaothong.hochiminhcity.gov.vn"

    def __init__(self, log_level="INFO"):
        super().__init__(log_level)
        self._cam_image_url = os.path.join(self._BASE_URL, "render/ImageHandler.ashx")
        self._cam_info_url = os.path.join(self._BASE_URL, "ajaxpro/VDMS.Web.Library.AJAX.FolderAjax,VDMS.Web.Library.ashx")
    

    def get_cam_info(self, page: int = 1, limit: int = -1) -> list[dict]:
        with requests.Session() as session:
            payload = {
                "path": "/root/vdms/tangthu/data/layerdata/camera",
                "isInTree": True,
                "searchKey": "",
                "layer": ["CAMERA"],
                "detail": True,
                "page": page,
                "limit": limit,
                "filterQuery": ["Publish:true AND CamStatus:UP"],
                "sortby": {"SortInfo": [
                        {
                            "Field": "ModifiedDate",
                            "Direction": 1
                        }
                    ]},
                "returnFields": ["DisplayName", "CamId", "Disctrict"]
            }

            headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Content-Type": "application/json; charset=UTF-8",
                "Origin": "https://giaothong.hochiminhcity.gov.vn",
                "Pragma": "no-cache",
                "Referer": "https://giaothong.hochiminhcity.gov.vn/",
                "sec-ch-ua-mobile": "?0",
                # "sec-ch-ua-platform": "Windows",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": self.ua_rotator.rotate(),
                "X-AjaxPro-Method": "SearchQuery"
            }

            with requests.Session() as session:
                _ = session.get(self._BASE_URL) # warmup to get cookies
                response = session.request("POST", self._cam_info_url, json=payload, headers=headers)
                with open("t.txt", "w") as f:
                    f.write(response.text)
                data = self._extract_cam_info(response.text)
                return data

    
    def _extract_cam_info(self, text) -> list[dict]:
        pattern = r'\[\{"__type":"VDMS\.Sense\.Helper\.Model\.FileProperty, VDMS\.Sense\.Helper\.Model.*?"Format":null\}\]'

        result = []
        matches = re.findall(pattern, text)
        for match in matches:
            json_data = json.loads(match)
            template = {data['Name']: data['Value'] for data in json_data}
            result.append(template)
        return result
    
    
    def crawl(self, id: str):
        headers = {
            "User-Agent": self.ua_rotator.get_random_ua(),
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
        }
        params = {
            "id": id
        }
        response = requests.get(self._BASE_URL, headers=headers, params=params)
        
        if response.status_code == 200:
            return response.content
        pass
        

if __name__ == "__main__":
    crawler = TrafficCrawler()
    data = crawler.get_cam_info()
    print(len(data))

    