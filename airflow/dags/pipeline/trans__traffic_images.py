from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.param import Param
from airflow.decorators import task
from hooks.minio_hook import MinioHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pytz
import logging

MINIO_CONN = 'conn_minio__datalake'
BUCKET = "datalake"
PSQL_TABLE = "tbl__raw__dim_cam_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"
CAM_ID = ["58af9a07bd82540010390c3b"]

params = {
    "id": Param(type="string",
                description="Traffic image id",
                examples=["58af9a07bd82540010390c3b"])
}

default_args = {
    "owner": "phonghuynh",
    "depends_on_past": False,
    "retries": 0
}

# Initialize the DAG
with DAG(
    'trans__traffic_images',  
    description='Crawling traffic image',
    schedule_interval="* * * * *",  
    start_date=datetime(2023, 4, 5),
    catchup=False,
    params=params
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    @task(provide_context=True)
    def image_crawling(id: str):
        from utils.crawling import TrafficCrawler

        crawler = TrafficCrawler()

        img_data = crawler.crawl(id)

        hook = MinioHook(conn_id=MINIO_CONN)

        now = datetime.now(pytz.timezone("Asia/Ho_Chi_Minh"))
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H-%M-%S")
        prefix = f"raw/traffic/{id}/{date_str}/{time_str}.jpg"

        try:
            hook.upload_img(bucket_name=BUCKET, prefix=prefix, image_data=img_data)
            logging.info(f"Image uploaded successfully to {BUCKET}/{prefix}")
        except Exception as e:
            logging.error(f"Failed to upload image to MinIO: {e}")
            raise
        

    end_task = DummyOperator(
        task_id='end'
    )

    image_crawling_task = image_crawling(id="{{params.id}}")

    start_task >> image_crawling_task >> end_task
