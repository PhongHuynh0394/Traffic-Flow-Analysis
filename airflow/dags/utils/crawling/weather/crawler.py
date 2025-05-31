from ..basecrawler import BaseCrawler
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import unicodedata
import requests
import os
import re
from typing import Optional

class WeatherCrawler(BaseCrawler):
    _BASE_URL = "https://www.accuweather.com"

    def __init__(self, proxy_token: Optional[str] = None):
        super().__init__(proxy_token=proxy_token)
        self._url_cities = os.path.join(self._BASE_URL, "vi/browse-locations/asi/vn")

        self._headers = {
            "Referer": "https://www.accuweather.com"
        }


    def crawl(self, url: str) -> dict[str, str]:
        result = {}
        self._headers.update({"User-Agent": self.ua_rotator.rotate()})
        with requests.Session() as session:
            try:
                r = session.get(url, headers=self._headers)
                r.raise_for_status()
            except Exception as e:
                proxies = self.proxy_manager.rotate(by_asn=False) if self.proxy_manager is not None else {}
                if proxies:
                    self.logger.error(f"Failed crawling, retry with proxy {proxies}")
                    r = session.get(url, headers=self._headers, proxies=proxies)
                    r.raise_for_status()
                else:
                    self.logger.error(f"Failed crawling, No proxy found ! unable to retry ! Error")
                    raise

        soup = BeautifulSoup(r.text, "html.parser")
        current_weather = soup.find("div", class_="current-weather-card card-module content-module")
        current_time = current_weather.find("p", class_="sub").text 
        status = current_weather.find("div", class_="phrase").text 
        temp_c = current_weather.find("div", class_="display-temp").text 
        properties = current_weather.find_all("div", class_="detail-item spaced-content")

        result.update({
            "current_time": current_time,
            "status": status,
            "temp_c": temp_c
        })

        for prop in properties:
            detail = prop.text.split("\n")
            detail = [_ for _ in detail if _ != ""]
            result.update({detail[0].lower(): detail[1]})

        return result


    def get_city_list(self) -> list[dict]:
        self._headers.update({"User-Agent": self.ua_rotator.rotate()})
    
        url = os.path.join(self._BASE_URL, "vi/browse-locations/asi/vn")
        headers = {
            "User-Agent": self.ua_rotator.rotate(),
            "Referer": "https://www.accuweather.com"
        }
        with requests.Session() as s:
            r = s.get(url, headers=headers)

        soup = BeautifulSoup(r.text, "html.parser")
        search_result = [tag for tag in soup.find_all("a", class_="search-result") if tag.get("class") == ["search-result"]]
        cities = [{"city": tag.text, "url": self._BASE_URL + tag["href"]} for tag in search_result]
        result = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self._get_district, city, headers) for city in cities]
            
            for future in as_completed(futures):
                try:
                    result.extend(future.result())
                except Exception as e:
                    self.logger.error(f"Error: {e}")

        return result


    def _preprocess(self, district: dict):
        # Process name
        d_name = district["district"].lower().replace(" ", "-").replace("quận", "district")
        d_name = unicodedata.normalize('NFD', d_name)
        d_name = ''.join(c for c in d_name if unicodedata.category(c) != 'Mn')
        d_name = d_name.replace('đ', 'd').replace('Đ', 'D')

        # Extract key
        match = re.search(r'key=(\d+)', district["district_url"])
        if match:
            key = match.group(1)
        
        return {"district_url": f"https://www.accuweather.com/en/vn/{d_name}/{key}/current-weather/{key}", "district": district['district']}
    

    def _get_district(self, city, headers):
        with requests.Session() as s:
            response = s.get(city['url'], headers=headers)
            soup = BeautifulSoup(response.text, "html.parser")
            districts = [self._preprocess({"district": tag.text, "district_url": tag.get("href")}) for tag in soup.find_all("a", class_="search-result") if tag.get("class") == ["search-result"]]
            districts = [{**district, **city} for district in districts]
            return districts

if __name__ == "__main__":
    crawler = WeatherCrawler()
    print(crawler.get_city_list())
    