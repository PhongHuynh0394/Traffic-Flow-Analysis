from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import task
from datetime import datetime
import logging


# Initialize the DAG
with DAG(
    'trans__weather_event',  
    description='Crawling weather event',
    schedule_interval=None,  
    start_date=datetime(2023, 4, 5),  
    catchup=False
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    @task(provide_context=True)
    def weather_crawling(**kwargs):
        from utils.crawling import WeatherCrawler

        crawler = WeatherCrawler()
        

    end_task = DummyOperator(
        task_id='end'
    )

    weather_crawling_task = weather_crawling()

    start_task >> weather_crawling_task >> end_task
