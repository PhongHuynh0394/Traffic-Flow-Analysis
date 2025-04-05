import os
from dotenv import load_dotenv
from airflow import settings
from airflow.models import Connection
from sqlalchemy.orm import sessionmaker
from airflow.utils.db import provide_session
from airflow.hooks.base_hook import BaseHook
import logging
load_dotenv()

@provide_session
def create_connection(session=None):

    # Minio conn
    minio_conn = {
        "conn_id": "conn_minio__datalake",
        "conn_type": "S3",
        "host": os.getenv("MINIO_ENDPOINT", "minio"),
        "login": os.getenv("MINIO_ROOT_USER", "minio"),
        "password": os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
        "schema": "",
        "port": os.getenv("MINIO_PORT", 9000)
    }

    # PSQL conn
    psql_conn = {
        "conn_id": "conn_psql__raw_crawl",
        "conn_type": "Postgres",
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "login": os.getenv("POSTGRES_USER", "airflow"),
        "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
        "schema": os.getenv("POSTGRES_DB", "raw_crawl"),
        "port": os.getenv("POSTGRES_PORT", 5432)
    }

    for conn_cf in [minio_conn, psql_conn]:

        # Check if the connection already exists
        conn = session.query(Connection).filter_by(conn_id=conn_cf["conn_id"]).first()
        
        if conn:
            logging.info(f"Connection {conn_cf['conn_id']} already exists")
        else:
            # Create a new connection
            new_conn = Connection(
                **conn_cf
            )
            session.add(new_conn)
            session.commit()
            logging.info(f"Connection {conn_cf['conn_id']} created successfully")



if __name__ == "__main__":
    create_connection()