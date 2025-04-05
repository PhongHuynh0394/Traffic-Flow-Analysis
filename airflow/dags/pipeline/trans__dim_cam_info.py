from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from airflow.decorators import task
from datetime import datetime
import logging

PSQL_TABLE = "tbl__raw__dim_cam_info"
PSQL_CONN_ID = "conn_raw_psql"


# Utils
def create_psql_table(hook):
    query = f"""
        CREATE TABLE IF NOT EXIST {PSQL_TABLE} (
            CamId varchar(255) PRIMARY KEY
            DisplayName varchar(255),
            District varchar(255)
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

    @task(provide_context=True)
    def cam_id_crawling(limit: int = -1):
        from utils.crawling import TrafficCrawler

        crawler = TrafficCrawler()
        return crawler.get_cam_info(limit=limit)
    

    @task()
    def ingest_psql(data: list[dict]):
        import pandas as pd
        
        # Create table
        hook = PostgresHook(postgres_conn_id=PSQL_CONN_ID)
        create_psql_table(hook)

        # Ingest
        try:
            df = pd.DataFrame(data)
            df.to_sql(PSQL_TABLE, hook.get_sqlalchemy_engine(), if_exists='replace', index=False)
        except Exception as e:
            logging.error(f"Failed to ingest data into {PSQL_TABLE}: {e}")
            raise


    end_task = DummyOperator(
        task_id='end'
    )

    cam_id = cam_id_crawling(limit="{{params.limit}}")
    ingest_psql_task = ingest_psql(cam_id)

    start_task >> cam_id >> ingest_psql_task >> end_task
