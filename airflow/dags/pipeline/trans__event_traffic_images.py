from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.param import Param
from airflow.decorators import task
from hooks.minio_hook import MinioHook
from hooks.redis_hook import RedisHook
from hooks.kafka_hook import KafkaProducerHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pytz
import logging
import pandas as pd
import requests
import pendulum
import requests
import json

MINIO_CONN = 'conn_minio__datalake'
REDIS_CONN_ID = "conn_redis"
BUCKET = "datalake"
PSQL_TABLE = "tbl__raw__dim_cam_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"
KAFKA_TOPIC = "traffic-object-raw"
MODEL_API = "http://object-counting-api:8000/upload-image"

params = {
    "district": Param(
                type="string",
                default="Quận 1",
                description="District of cameras",
                examples=["Quận 1"])
}

default_args = {
    "owner": "phonghuynh",
    "depends_on_past": False,
    "retries": 0
}

kafka_config={
    "bootstrap.servers": "kafka-broker-1:9094",
    "replication_factor": 1,
    "num_partitions": 1,
}

# Initialize the DAG
with DAG(
    'trans__event_traffic_images',  
    description='Crawling traffic image',
    schedule_interval="* * * * *",  
    start_date=pendulum.datetime(2023, 4, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    params=params
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    @task
    def get_cam_id(district: str = "Quận 1") -> list[str]:
        from utils.preprocess import standard_location

        district_key = standard_location(district)

        key = f"cam:district:{district_key}"
        redis_hook = RedisHook(REDIS_CONN_ID)
        data = redis_hook.get(key)
        if not data: 
            # Get cam id from psql
            psql_hook = PostgresHook(postgres_conn_id=PSQL_CONN_ID)
            query = f"SELECT id FROM {PSQL_TABLE} WHERE district = '{district}'"
            data = psql_hook.get_records(query)
            data = [i[0] for i in data]

            # cache redis
            data = redis_hook.set(key, data)

        return data


    @task(provide_context=True)
    def image_crawling(id: str):
        from utils.crawling import TrafficCrawler

        # Crawl raw image
        crawler = TrafficCrawler()
        img_data = crawler.crawl(id)

        # Save raw img to S3
        s3_hook = MinioHook(conn_id=MINIO_CONN)

        # now = datetime.now(pytz.timezone("Asia/Ho_Chi_Minh"))
        # date_str = now.strftime("%Y-%m-%d")
        # time_str = now.strftime("%H-%M-%S")
        # timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

        now = pendulum.now("Asia/Ho_Chi_Minh")
        date_str = now.to_date_string() # 'YYYY-MM-DD'
        time_str = now.format("HH-mm-ss") # 'HH-MM-SS'
        timestamp_str = now.to_datetime_string() # 'YYYY-MM-DD HH:MM:SS'

        prefix = f"raw/traffic/{id}/{date_str}/{time_str}.jpg"

        try:
            s3_hook.upload_img(bucket_name=BUCKET, prefix=prefix, image_data=img_data)
            logging.info(f"Image uploaded successfully to {BUCKET}/{prefix}")
        except Exception as e:
            logging.error(f"Failed to upload image to MinIO: {e}")
            raise

        # Predict with api
        files = {'file': ('file.png', img_data, 'image/png')}
        message = requests.post(MODEL_API, files=files).json()
        message.update({
            "timestamp": timestamp_str,
            "cam_id": id,
            "img": f"{BUCKET}/{prefix}"
        })
            
        kafka_hook = KafkaProducerHook(config=kafka_config)
        kafka_hook.produce(
            topic=KAFKA_TOPIC,
            value=message,
            isflush=True
        )


    end_task = DummyOperator(
        task_id='end'
    )

    cam_id = get_cam_id(district="{{params.district}}")
    image_crawling_task = image_crawling.expand(id=cam_id)

    start_task >> image_crawling_task >> end_task
