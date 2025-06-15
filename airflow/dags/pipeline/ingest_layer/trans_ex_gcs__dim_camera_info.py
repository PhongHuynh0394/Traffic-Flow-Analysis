from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param
from airflow.decorators import task
from hooks.gcs_hook import GCSHook

import pendulum
import pandas as pd
import logging
import os


GCS_BUCKET = "traffic_flow_thesis"
GCS_CREDENTIAL_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
TABLE_NAME = "tbl__raw__dim_cam_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"

default_args = {
    "owner": "PhongHuynh0394",
    "depends_on_past": False,
    "retries": 0
}


with DAG(
    'trans_ex_gcs__dim_camera_info',
    default_args=default_args,
    description='Crawling traffic image id',
    schedule_interval="@once",  
    start_date=pendulum.datetime(2023, 4, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False,  
    params={"limit": Param(default=-1, type="integer")},
    tags=["crawl", "dim", "psql", "gcs"]
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    create_table_psql = SQLExecuteQueryOperator(
        task_id="create_psql_table",
        conn_id=PSQL_CONN_ID,
        sql=f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    id varchar(255) PRIMARY KEY, 
                    location varchar(255), 
                    district varchar(255),
                    longitude varchar(255),
                    latitude varchar(255)
                );"""
    )


    @task(provide_context=True)
    def crawl__raw_camera_info(limit: int = -1):
        from utils.crawling import TrafficCrawler
        from utils.crawling.traffic.constant import CAMERA_DISTRICT_MAPPING

        crawler = TrafficCrawler()
        data = crawler.get_cam_info(limit=limit)

        df = pd.DataFrame(data)
        df.rename(columns={
            "CamId": "id",
            "DisplayName": "location",
            "Disctrict": "district"
        }, inplace=True)
        df['district'] = df['district'].fillna(df['id'].map(CAMERA_DISTRICT_MAPPING))
        # df.fillna(value="unknown", inplace=True)
        df = df[['id', 'location', 'district', 'longitude', 'latitude']]

        temp_csv_path = '/tmp/temp_data.tsv'
        df.to_csv(temp_csv_path, sep='\t', index=False, header=False)
        logging.info(f"Write {len(df)} data to {temp_csv_path}")

        return temp_csv_path


    @task
    def ingest__gcs_ch(data_path: str):
        
        # Create table
        hook = PostgresHook(postgres_conn_id=PSQL_CONN_ID)

        # TRUNCATE
        hook.run(f"TRUNCATE TABLE {TABLE_NAME};")

        try:
            hook.bulk_load(table=TABLE_NAME, tmp_file=data_path)
            logging.info(f"Successfully loaded data into PSQL table: {TABLE_NAME}")
        except Exception as e:
            logging.error(f"Failed to load data into PSQL {TABLE_NAME}: {e}")
            raise

        try:
            gcs_hook = GCSHook(
                gcs_credential_env=GCS_CREDENTIAL_ENV,
                bucket=GCS_BUCKET
            )
            prefix = f"raw/raw_dim/{TABLE_NAME}.tsv"
            gcs_hook.upload_file(file_path=data_path, destination_blob_name=prefix)
        except Exception as e:
            logging.error(f"Failed to upload data to GCS: {e}")
            raise
        finally:
            if os.path.exists(data_path):
                logging.info(f"Delete temp file {data_path}")
                os.remove(data_path)


    end_task = DummyOperator(
        task_id='end'
    )

    cam_id = crawl__raw_camera_info(limit="{{params.limit}}")
    ingesting_task = ingest__gcs_ch(cam_id)

    start_task >> create_table_psql >> cam_id >> ingesting_task >> end_task
