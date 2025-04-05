import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from datetime import datetime


# Initialize the DAG
with DAG(
    'trans__traffic_images',  
    description='Crawling traffic image',
    schedule_interval=None,  
    start_date=datetime(2023, 4, 5),  
    catchup=False,  
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    @task()
    def image_crawling():
        from utils.crawling import WeatherCrawler, TrafficCrawler
        import logging
        crawler = TrafficCrawler()
        return crawler.get_cam_info(limit=10)
        

    end_task = DummyOperator(
        task_id='end'
    )

    image_crawling_task = image_crawling()

    # Define the task dependencies (order in which tasks will run)
    start_task >> image_crawling_task >> end_task
