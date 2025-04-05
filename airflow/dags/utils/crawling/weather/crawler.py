from ..basecrawler import BaseCrawler

class WeatherCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()


    def crawl(self):
        pass
    
    def rotate_ua(self):
        return self.ua_rotator.rotate()
    

if __name__ == "__main__":
    crawler = WeatherCrawler()
    print(crawler.rotate_ua())