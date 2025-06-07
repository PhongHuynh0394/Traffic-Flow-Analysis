from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator

import pendulum
import logging

from utils.spark.spark_io import SparkIO
from pyspark import SparkConf
from pyspark.sql.functions import col


GCS_BUCKET = "traffic_flow_thesis"
GCS_RAW_PATH = "raw"
GCS_CLEANED_PATH = "cleaned"

default_args = {
    "owner": "PhongHuynh0394",
    "depends_on_past": False,
    "retries": 3
}

packages = [
    "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"
]

spark_conf = (SparkConf()
                .set("spark.executor.memory", "4g")
                .set("spark.jars", ",".join(packages))
                .setMaster("local[*]")
)

@task
def trans__raw__dim_cam_info():
    # gs_path = f"gs://{GCS_BUCKET}/{GCS_RAW_PATH}/raw_dim/tbl__raw__dim_cam_info.tsv"
    gs_path = f"gs://{GCS_BUCKET}/raw_dim/tbl__raw__dim_cam_info.tsv"
    with SparkIO(conf=spark_conf, gcs=True) as spark:
        try:
            logging.info(f"Read data from {gs_path}")
            dim_cam_info = spark.read \
                            .option("delimiter", "\t") \
                            .option("header", False) \
                            .csv(gs_path)
        except Exception as e:
            logging.error(f"Fail to read data from GCS, error: {e}")
            raise

        dim_cam_columns = ["id", "location", "district", "longitude", "latitude"]
        dim_cam_info_cleaned = dim_cam_info.toDF(*dim_cam_columns)
        dim_cam_info_cleaned = dim_cam_info_cleaned.withColumn("latitude", col("latitude").cast("double")) \
                                                    .withColumn("longitude", col("longitude").cast("double"))
        


        cleaned_gs_path = f"gs://{GCS_BUCKET}/{GCS_CLEANED_PATH}/dim_cam_info_cleaned_v1"
        try:
            logging.info(f"Write data to {cleaned_gs_path}")
            dim_cam_info_cleaned.write \
                .mode("overwrite") \
                .parquet(cleaned_gs_path)
        except Exception as e:
            logging.error(f"Failed writting data to {cleaned_gs_path}, error: {e}")
            raise

with DAG(
    'trans_gcs_gcs__dim_cam_info',
    description='Transform data in batch layer',
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=pendulum.datetime(2025, 5, 8, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=['batch', 'gcs', 'transform']
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    trans__raw__dim_cam_info_task = trans__raw__dim_cam_info()

    end = DummyOperator(
        task_id='end'
    )

    start >> trans__raw__dim_cam_info_task >> end