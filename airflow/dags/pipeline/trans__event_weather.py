from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import task
from datetime import datetime
# import pendulum
import logging

URL = [
    "https://www.accuweather.com/en/vn/district-1/3554433/current-weather/3554433"
]


# Initialize the DAG
with DAG(
    'trans__weather_event',  
    description='Crawling weather event',
    schedule_interval="* * * * *",  
    start_date=datetime(2023, 4, 5),  
    catchup=False
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )


    @task(provide_context=True, multiple_outputs=True)
    def weather_crawling(**kwargs):
        from utils.crawling import WeatherCrawler

        crawler = WeatherCrawler()
        data = crawler.crawl(URL[0])
        logging.info(data)
        return data
        

    end_task = DummyOperator(
        task_id='end'
    )

    weather_crawling_task = weather_crawling()

    start_task >> weather_crawling_task >> end_task
