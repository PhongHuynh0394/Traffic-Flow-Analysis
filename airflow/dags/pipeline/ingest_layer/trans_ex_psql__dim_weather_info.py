from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task

import pendulum
import pandas as pd
import logging
import os
import time


PSQL_TABLE = "tbl__raw__dim_weather_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"


default_args = {
    "owner": "PhongHuynh0394",
    "depends_on_past": False,
    "retries": 0
}


with DAG(
    'trans_ex_gcs__dim_weather_info',  
    default_args=default_args,
    description='Crawl dim weather info',
    schedule_interval=None,  
    start_date=pendulum.datetime(2023, 4, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["crawl", "dim"]
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    create_table_psql = SQLExecuteQueryOperator(
        task_id="create_psql_table",
        conn_id=PSQL_CONN_ID,
        sql = f"""
            CREATE TABLE IF NOT EXISTS {PSQL_TABLE} (
                id SERIAL PRIMARY KEY,
                district_url TEXT,
                district varchar(255),
                city varchar(255),
                url TEXT
            )
        """
    )


    @task(provide_context=True, retries=3, retry_delay=pendulum.duration(seconds=5))
    def weather_id_crawling():
        from utils.crawling import WeatherCrawler

        crawler = WeatherCrawler()
        data = crawler.get_city_list()

        df = pd.DataFrame(data)

        return df[["district_url", "district", "city", "url"]]


    @task()
    def ingest_psql(data: pd.DataFrame):
        
        # Create table
        hook = PostgresHook(postgres_conn_id=PSQL_CONN_ID)

        # TRUNCATE
        hook.run(f"TRUNCATE TABLE {PSQL_TABLE};")
        
        temp_path = '/tmp/temp_weather.tsv'
        data.to_csv(temp_path, sep='\t', index=True, header=False)
        logging.info(f"Write data to {temp_path}")

        try:
            hook.bulk_load(table=PSQL_TABLE, tmp_file=temp_path)
            logging.info(f"Successfully loaded data into {PSQL_TABLE}")
        except Exception as e:
            logging.error(f"Failed to load data into {PSQL_TABLE}: {e}")
            raise
        finally:
            if os.path.exists(temp_path):
                logging.info(f"Delete temp file {temp_path}")
                os.remove(temp_path)


    end_task = DummyOperator(
        task_id='end'
    )

    weather_id = weather_id_crawling()
    ingest_psql_task = ingest_psql(weather_id)

    start_task >> create_table_psql >> weather_id >> ingest_psql_task >> end_task
