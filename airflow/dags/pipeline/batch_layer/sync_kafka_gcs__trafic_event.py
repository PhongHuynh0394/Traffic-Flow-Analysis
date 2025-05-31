from airflow import DAG
from operators.kafka2gcs import KafkaToGCSOperator
import pendulum

GCS_BUCKET = "traffic_flow_thesis"
GCS_CREDENTIAL_ENV = "GOOGLE_APPLICATION_CREDENTIALS"
KAFKA_TOPIC = "traffic-object-raw"

kafka_config = {
    "bootstrap.servers": "kafka-broker-1:9094",
    'group.id': 'traffic-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
}

default_args = {
    "owner": "PhongHuynh0394",
    "depends_on_past": False,
    "retries": 0
}

with DAG(
    'sync_kafka_gcs__trafic_event',  
    description='Sync Kafka to GCS',
    default_args=default_args,
    schedule_interval="0,30 * * * *",  
    start_date=pendulum.datetime(2023, 4, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=['stream', 'consumer', 'kafka', 'gcs']
) as dag:

    kafka_to_gcs = KafkaToGCSOperator(
        task_id='kafka_to_gcs_traffic_event',
        gcs_bucket=GCS_BUCKET,
        kafka_topic=KAFKA_TOPIC,
        kafka_config=kafka_config,
        gcs_credential_env=GCS_CREDENTIAL_ENV,
        # prefix="raw/raw_event",
        prefix="raw/raw_event/sample",
        poll_timeout=30,
        max_messages=1000,
        partition_by="timestamp",
        partition_level="hour"
    )

