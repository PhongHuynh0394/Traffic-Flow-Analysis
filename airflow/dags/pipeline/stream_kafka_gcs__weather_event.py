from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from hooks.redis_hook import RedisHook
from hooks.kafka_hook import KafkaProducerHook
from airflow.models.param import Param
from operators.kafka2gcs import KafkaToGCSOperator
from datetime import datetime
import pendulum
import logging

GCS_BUCKET = "traffic_flow_thesis"
GCS_CREDENTIAL_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
KAFKA_TOPIC = "weather-raw"

kafka_config = {
    "bootstrap.servers": "kafka-broker-1:9094",
    'group.id': 'weather-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
}

with DAG(
    'stream_kafka_gcs__weather_event',  
    description='Sync Kafka to GCS',
    schedule_interval="* * * * *",  
    start_date=pendulum.datetime(2023, 4, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
) as dag:

    kafka_to_gcs = KafkaToGCSOperator(
        task_id='kafka_to_gcs_weather_event',
        gcs_bucket=GCS_BUCKET,
        kafka_topic=KAFKA_TOPIC,
        kafka_config=kafka_config,
        gcs_credential_env=GCS_CREDENTIAL_ENV,
        prefix="raw_event",
        poll_timeout=60,
        max_messages=1000
    )

