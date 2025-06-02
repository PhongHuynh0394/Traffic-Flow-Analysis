from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
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

# MINIO_CONN = 'conn_minio__datalake'
GCS_CREDENTIAL_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
REDIS_CONN_ID = "conn_redis"
GCS_BUCKET = "traffic_flow_thesis"
PSQL_TABLE = "tbl__raw__dim_cam_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"
KAFKA_TOPIC = "traffic-object-raw"
MODEL_API = "http://object-counting-api:8000/model/object_counting/predict"
# MODEL_API = "http://object-counting-api:8000/upload-image"

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

kafka_config={
    "bootstrap.servers": "kafka-broker-1:9094",
    "replication_factor": 1,
    "num_partitions": 1,
}

MAPPING_FIX_CAM = {'Huyện Bình Chánh': ['5ad06a0d98d8fc001102e27b',
  '662a87df1afb9c00172d2522',
  '6792f03d8c5ed4001b27f378'],
 'Quận 1': ['65e054fb6b18080018db6632',
  '662b85bf1afb9c00172dd149',
  '662b811d1afb9c00172dcc1d',
  '662b7d0c1afb9c00172dc6a6',
  '662b82da1afb9c00172dce94'],
 'Quận 10': ['63ae7a74bfd3d90017e8f2c7', '6623e7076f998a001b2523ea'],
 'Quận 12': ['595dd7693dcfc400106f28b0'],
 'Quận 3': ['5deb576d1dc17d7c5515ad0e',
  '5deb576d1dc17d7c5515ad11',
  '662b80e81afb9c00172dcbec',
  '63ae73cebfd3d90017e8f00d',
  '5deb576d1dc17d7c5515acf8'],
 'Quận 4': ['63ae76ddbfd3d90017e8f11b'],
 'Quận 5': ['66f1266f538c780017c93579',
  '66b1c190779f740018673ed4',
  '63b3c274bfd3d90017e9ab93',
  '662b4efc1afb9c00172d86bc'],
 'Quận 7': ['662a8ef41afb9c00172d2af2', '662a8c931afb9c00172d2901'],
 'Quận 9': ['63b54996bfd3d90017ea781a',
  '63b54938bfd3d90017ea77f6',
  '59d3414302eb490011a0a610'],
 'Quận Bình Thạnh': ['6623e7b76f998a001b25242d',
  '63b66051bfd3d90017eaa4a3',
  '5a8255a55058170011f6eac7',
  '5d9dddb9766c880017188c96'],
 'Quận Bình Tân': ['662a881a1afb9c00172d2559', '662b51201afb9c00172d889a'],
 'Quận Gò Vấp': ['5a6066608576340017d06617', '6623ed9b6f998a001b2526cd'],
 'Quận Thủ Đức': ['5d8cd7bb766c880017188952', '5d8cd653766c88001718894c'],
 'Quận Tân Bình': ['5deb576d1dc17d7c5515ad08',
  '66b1c4e7779f7400186741e4',
  '5deb576d1dc17d7c5515ad09'],
 'Quận Tân Phú': ['6623f1046f998a001b2527db'],
 'Quận Phú Nhuận': ['6623e8da6f998a001b2524a6'],
 'Huyện Hóc Môn': ['6623ef2b6f998a001b252753', '6623efc26f998a001b25277f'],
 'Quận 2': ['649da495a6068200171a6cb6', '63b5503bbfd3d90017ea7ccc'],
 'Quận 6': ['5d8cd326766c88001718893e',
  '662b4f7e1afb9c00172d872e',
  '66b1c22f779f740018673f6e'],
 'Quận 11': ['5a824c905058170011f6eab0'],
 'Huyện Nhà Bè': ['5d9de3c2766c880017188cb3']}

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
    render_template_as_native_obj=True
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
    def image_crawling(id: str, district: str):
        from utils.crawling import TrafficCrawler

        # Crawl raw image
        crawler = TrafficCrawler()
        img_data = crawler.crawl(id)

        # Save raw img to S3
        # s3_hook = MinioHook(conn_id=MINIO_CONN)
        gcs_hook = GCSHook(
            gcs_credential_env=GCS_CREDENTIAL_ENV,
            bucket=GCS_BUCKET
        )

        now = pendulum.now("Asia/Ho_Chi_Minh")
        date_str = now.to_date_string() # 'YYYY-MM-DD'
        time_str = now.format("HH-mm-ss") # 'HH-MM-SS'
        timestamp_str = now.to_datetime_string() # 'YYYY-MM-DD HH:MM:SS'

        prefix = f"raw/raw_images/traffic/{id}/{date_str}/{time_str}.jpg"

        # try:
        #     # s3_hook.upload_img(bucket_name=BUCKET, prefix=prefix, image_data=img_data)
        #     gcs_hook.upload_bytes(data=img_data, destination_blob_name=prefix)
        # except Exception as e:
        #     logging.error(f"Failed to upload image to GCS: {e}")
        #     raise

        # Predict with api
        files = {'file': ('file.png', img_data, 'image/png')}
        message = requests.post(MODEL_API, files=files).json()
        message.update({
            "timestamp": timestamp_str,
            "cam_id": id,
            "img": f"{GCS_BUCKET}/{prefix}",
            "district": district
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

    # for district in dag.params["district"]:
    for district in MAPPING_FIX_CAM:
        # district_name = district.replace("Quận ", "district_")
        district_name = district.replace(" ", "_").replace("Quận", "district").replace("Huyện", "district")

        # cam_id = query_cam_id.override(
        #     task_id=f"query_cam__{district_name}"
        # )(district=district, limit=dag.params["limit"])

        cam_id = MAPPING_FIX_CAM[district]

        image_crawling_task = image_crawling.override(task_id=f"crawling__{district_name}") \
                                            .partial(district=district) \
                                            .expand(id=cam_id)

        start_task >> image_crawling_task >> end_task