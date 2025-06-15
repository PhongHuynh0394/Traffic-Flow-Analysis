from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import task
from hooks.minio_hook import MinioHook
from hooks.gcs_hook import GCSHook
from hooks.redis_hook import RedisHook
from hooks.kafka_hook import KafkaProducerHook
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import requests
import pendulum
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

import random
import json
from datetime import datetime
from uuid import uuid4

from utils.crawling.traffic.constant import MAPPING_FIX_CAM
# from utils.preprocess import standard_location

# MINIO_CONN = 'conn_minio__datalake'
GCS_CREDENTIAL_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
REDIS_CONN_ID = "conn_redis"
GCS_BUCKET = "traffic_flow_thesis"
PSQL_TABLE = "tbl__raw__dim_cam_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"
KAFKA_TOPIC = "traffic-object-raw"

# REDPANDA_SERVER = Variable.get("rpanda__server")
# REDPANDA_USER = Variable.get("rpanda__user")
# REDPANDA_PASS = Variable.get("rpanda__password")
MODEL_API = "http://object-counting-api:8000/model/object_counting/predict"
# MODEL_API = Variable.get("model__api")

params = {
    "district": Param(
                type="array",
                default=["Quận 1", "Quận 3", "Quận 5", "Quận 10"],
                description="District of cameras",
                examples=["Quận 1"]),
    "limit": Param(
                type="integer",
                default=5,
                description="Number of cameras to crawl"
    )
}

default_args = {
    "owner": "PhongHuynh0394",
    "depends_on_past": False,
    "retries": 0
}

conf ={
    "bootstrap.servers": "kafka-broker-1:9094",
    "replication_factor": 1,
    "num_partitions": 1,
}

# conf = {
#     "bootstrap.servers": REDPANDA_SERVER,
#     'security.protocol': 'SASL_SSL',
#     'sasl.mechanism': 'SCRAM-SHA-256',
#     'sasl.username': REDPANDA_USER,
#     'sasl.password': REDPANDA_PASS,
# }

def get_mock():

    width, height = 512, 288
    classes = [
        {"name": "car", "class_id": 2},
        {"name": "truck", "class_id": 7},
        {"name": "motorbike", "class_id": 3},
        {"name": "bus", "class_id": 5}
    ]

    num_objects = random.randint(1, 10)
    objects = []
    for _ in range(num_objects):
        cls = random.choice(classes)
        x1 = random.uniform(0, width * 0.9)
        y1 = random.uniform(0, height * 0.9)
        x2 = x1 + random.uniform(10, 100)
        y2 = y1 + random.uniform(10, 100)
        x2 = min(x2, width)
        y2 = min(y2, height)

        obj = {
            "class_object": cls["name"],
            "coordinates": [x1, y1, x2, y2],
            "confidence": round(random.uniform(0.3, 0.95), 2),
            "class_id": cls["class_id"],
            "classname": cls["name"]
        }
        objects.append(obj)

    data = {
        "image_shape": [height, width],
        "total": num_objects,
        "objects": objects,
    }

    return data


# Initialize the DAG
with DAG(
    'trans_ex_kafka__raw_traffic',
    description='Crawling traffic image',
    default_args=default_args,
    schedule_interval="* * * * *",  
    start_date=pendulum.datetime(2023, 4, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    params=params,
    tags=["producer", "raw"],
    render_template_as_native_obj=True,
    max_active_runs=1,
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    @task
    def query_cam_id(district: str = "Quận 1", limit: int = 5) -> list[str]:
        from utils.preprocess import standard_location

        district_key = standard_location(district)

        key = f"cam:district:{district_key}"
        redis_hook = RedisHook(REDIS_CONN_ID)
        data = redis_hook.get(key)
        if not data: 
            # Get cam id from psql
            logging.info("Get cam id from psql")
            psql_hook = PostgresHook(postgres_conn_id=PSQL_CONN_ID)
            query = f"SELECT id FROM {PSQL_TABLE} WHERE district = '{district}' LIMIT {limit}"
            data = psql_hook.get_records(query)
            data = [i[0] for i in data]

            # cache redis
            data = redis_hook.set(key, data, ttl=300)

        return data


    @task(provide_context=True)
    def image_crawling():
        from utils.crawling import TrafficCrawler

        def crawling_traffic_frame(id, district):
            # Crawl raw image
            # crawler = TrafficCrawler()
            # img_data = crawler.crawl(id)

            # Save raw img to S3
            # s3_hook = MinioHook(conn_id=MINIO_CONN)
            # gcs_hook = GCSHook(
            #     gcs_credential_env=GCS_CREDENTIAL_ENV,
            #     bucket=GCS_BUCKET
            # )

            now = pendulum.now("Asia/Ho_Chi_Minh")
            date_str = now.to_date_string() # 'YYYY-MM-DD'
            time_str = now.format("HH-mm-ss") # 'HH-MM-SS'
            # timestamp_str = now.to_datetime_string() # 'YYYY-MM-DD HH:MM:SS'
            timestamp_str = now.to_iso8601_string()  # e.g. '2025-06-01T20:29:07+07:00'

            prefix = f"raw/raw_images/traffic/{id}/{date_str}/{time_str}.jpg"

            # try:
            #     # s3_hook.upload_img(bucket_name=BUCKET, prefix=prefix, image_data=img_data)
            #     gcs_hook.upload_bytes(data=img_data, destination_blob_name=prefix)
            # except Exception as e:
            #     logging.error(f"Failed to upload image to GCS: {e}")
            #     raise

            # Predict with api
            # files = {'file': ('file.png', img_data, 'image/png')}
            # message = requests.post(MODEL_API, files=files)
            # message.raise_for_status()
            # message = message.json()
            message = get_mock()
            message.update({
                "timestamp": timestamp_str,
                "cam_id": id,
                "img": f"{GCS_BUCKET}/{prefix}",
                "district": district
            })
                
            kafka_hook = KafkaProducerHook(config=conf)
            kafka_hook.produce(
                topic=KAFKA_TOPIC,
                value=message,
                isflush=True
            )

            return message
        
        with ThreadPoolExecutor(max_workers=25) as executor:
            futures = {executor.submit(crawling_traffic_frame, id, district): id for id, district in MAPPING_FIX_CAM}

            for future in as_completed(futures):
                try:
                    logging.info(f"Success full process for cam {futures[future]}")
                except Exception as e:
                    logging.error(f"Error at {futures[future]}, error: {e}")


    end_task = DummyOperator(
        task_id='end'
    )

    # for district in dag.params["district"]:
    # for district in MAPPING_FIX_CAM:
    #     # district_name = district.replace("Quận ", "district_")
    #     district_name = district.replace(" ", "_").replace("Quận", "district").replace("Huyện", "district")

        # cam_id = query_cam_id.override(
        #     task_id=f"query_cam__{district_name}"
        # )(district=district, limit=dag.params["limit"])

        # cam_id = MAPPING_FIX_CAM[district]

        # image_crawling_task = image_crawling.override(task_id=f"crawling__{district_name}") \
        #                                     .partial(district=district) \
        #                                     .expand(id=cam_id)

    image_crawling_task = image_crawling()

    start_task >> image_crawling_task >> end_task