from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.param import Param
from airflow.decorators import task
from hooks.minio_hook import MinioHook
from datetime import datetime
import logging

MINIO_CONN = 'conn_minio__datalake'

# Initialize the DAG
with DAG(
    'trans__traffic_images',  
    description='Crawling traffic image',
    schedule_interval=None,  
    start_date=datetime(2023, 4, 5),  
    catchup=False
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    @task(provide_context=True)
    def image_crawling(**kwargs):
        from utils.crawling import TrafficCrawler
        crawler = TrafficCrawler()

        hook = MinioHook(minio_conn_id=MINIO_CONN)

        

    end_task = DummyOperator(
        task_id='end'
    )

    image_crawling_task = image_crawling()

    start_task >> image_crawling_task >> end_task
