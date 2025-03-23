from abc import ABC, abstractmethod
import logging
import os

class BaseCrawler(ABC):

    def __init__(self, log_level="INFO"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger(log_level)

    
    @abstractmethod
    def crawl(self):
        raise NotImplementedError(f"{self.__class__.__name__} must have crawl method")       
    

    def setup_logger(self, log_level="INFO"):
        log_level = getattr(logging, log_level, logging.INFO)
        self.logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(console_handler)
            self.logger.propagate = False # prevent duplicate log
        
