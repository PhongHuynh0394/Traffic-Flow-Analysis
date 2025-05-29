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

with DAG(
    'sync_kafka_gcs__trafic_event',  
    description='Sync Kafka to GCS',
    schedule_interval="* * * * *",  
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
        prefix="raw/raw_event",
        poll_timeout=60,
        max_messages=100
    )

