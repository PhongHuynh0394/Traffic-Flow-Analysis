from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param
from airflow.decorators import task
from datetime import datetime
import pandas as pd
import logging
import os
import time

PSQL_TABLE = "tbl__raw__dim_cam_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"

default_args = {
    "owner": "phonghuynh",
    "depends_on_past": False,
    "retries": 0
}


# Utils
def create_psql_table(hook):
    query = f"""
        CREATE TABLE IF NOT EXISTS {PSQL_TABLE} (
            id varchar(255) PRIMARY KEY,
            location varchar(255),
            district varchar(255)
        )
    """
    try:
        hook.run(query)
        logging.info(f"Creat table if not exist {PSQL_TABLE}")
    except Exception as e:
        logging.error(f"Failed to create table {PSQL_TABLE}: {e}")
        raise


with DAG(
    'trans__traffic_image_id',  
    description='Crawling traffic image id',
    schedule_interval=None,  
    start_date=datetime(2023, 4, 5),  
    catchup=False,  
    params={"limit": Param(default=-1, type="integer")}
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    create_table_psql = SQLExecuteQueryOperator(
        task_id="create_psql_table",
        conn_id=PSQL_CONN_ID,
        sql=f"CREATE TABLE IF NOT EXISTS {PSQL_TABLE} (id varchar(255) PRIMARY KEY, location varchar(255), district varchar(255));"
    )


    @task(provide_context=True)
    def cam_id_crawling(limit: int = -1):
        from utils.crawling import TrafficCrawler

        crawler = TrafficCrawler()
        data = crawler.get_cam_info(limit=limit)

        df = pd.DataFrame(data)
        df.rename(columns={
            "CamId": "id",
            "DisplayName": "location",
            "Disctrict": "district"
        }, inplace=True)
        df.fillna(value="unknown", inplace=True)

        return df


    @task()
    def ingest_psql(data: pd.DataFrame):
        
        # Create table
        hook = PostgresHook(postgres_conn_id=PSQL_CONN_ID)

        # TRUNCATE
        hook.run(f"TRUNCATE TABLE {PSQL_TABLE};")
        
        temp_csv_path = '/tmp/temp_data.tsv'
        data.to_csv(temp_csv_path, sep='\t', index=False, header=False)
        logging.info(f"Write data to {temp_csv_path}")

        try:
            time.sleep(100)
            hook.bulk_load(table=PSQL_TABLE, tmp_file=temp_csv_path)
            logging.info(f"Successfully loaded data into {PSQL_TABLE}")
        except Exception as e:
            logging.error(f"Failed to load data into {PSQL_TABLE}: {e}")
            raise
        finally:
            if os.path.exists(temp_csv_path):
                logging.info(f"Delete temp file {temp_csv_path}")
                os.remove(temp_csv_path)


    end_task = DummyOperator(
        task_id='end'
    )

    cam_id = cam_id_crawling(limit="{{params.limit}}")
    ingest_psql_task = ingest_psql(cam_id)

    start_task >> create_table_psql >> cam_id >> ingest_psql_task >> end_task
