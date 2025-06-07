from google.cloud import storage
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.oauth2 import service_account
from confluent_kafka import Consumer, KafkaException
from collections import defaultdict
import pyarrow as pa
import pyarrow.parquet as pq
import pendulum
import json
import os
from io import BytesIO
import gzip
from dotenv import load_dotenv
import uuid

load_dotenv()

class KafkaToGCSOperator(BaseOperator):

    @apply_defaults
    def __init__(self, 
                 gcs_bucket: str,
                 kafka_topic: str,
                 kafka_config: dict,
                 prefix: str = "raw_event",
                 poll_timeout: int = 30, # sync in minutes
                 max_messages: int = 1000,
                 gcs_credential_env: str = "GOOGLE_APPLICATION_CREDENTIALS",
                 partition_by: str = "timestamp",
                 partition_level: str = "hour",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        # GCS
        self.gcs_credential_env = gcs_credential_env
        self.bucket_name = gcs_bucket
        self.gcs_client = self.__get_gcs_client()
        self.partition_by = partition_by
        self.partition_level = partition_level

        # Kafka
        self.kafka_topic = kafka_topic
        self.prefix = prefix
        self.poll_timeout = poll_timeout
        self.max_messages = max_messages
        self.kafka_config = kafka_config


    def __get_gcs_client(self):
        gcs_env = os.getenv(self.gcs_credential_env)
        if not gcs_env:
            raise ValueError(f"Environment variable {self.gcs_credential_env} is not set.")
        credential_info = json.loads(gcs_env)
        credentials = service_account.Credentials.from_service_account_info(credential_info)
        return storage.Client(credentials=credentials, project=credential_info.get("project_id"))
    

    def __consume_kafka_message(self):
        self.log.info("Start Consuming Kafka messages")
        consumer = Consumer(self.kafka_config)
        consumer.subscribe([self.kafka_topic])

        messages = []
        # start_time = datetime.utcnow()
        last_msg_time = pendulum.now("Asia/Ho_Chi_Minh")

        try:
            while len(messages) < self.max_messages:
                current_time = pendulum.now("Asia/Ho_Chi_Minh") 
                msg = consumer.poll(timeout=10)
                if msg is None:
                    # no message left within poll_timout
                    if (current_time - last_msg_time).seconds > self.poll_timeout:
                        break
                    continue

                if msg.error():
                    # add error handling for kafka 
                    raise KafkaException(msg.error())

                try:
                    decoded = msg.value().decode('utf-8')
                    json_msg = json.loads(decoded)
                    messages.append(json_msg)
                    last_msg_time = current_time # reset polling time
                except Exception as e:
                    self.log.warning(f"Skipping invalid message: {e}")
                    raise
        finally:
            consumer.close()
        
        return messages


    def __write_to_gcs(self, messages: list):
        if not messages:
            self.log.info("No messages to write to GCS.")
            return

        # now = datetime.utcnow()
        partitions = defaultdict(list)

        # Group messages by partition
        for message in messages:
            ts = pendulum.parse(message.get(self.partition_by))
            partition_level_map = {
                "year": ts.year,
                "month": ts.month,
                "day": ts.day,
                "hour": ts.hour
            }
            partition_path = f"{self.prefix}/{self.kafka_topic}/"

            for level, value in partition_level_map.items():

                if level == "year":
                    partition_path = os.path.join(partition_path, f"{level}={value}")
                else:
                    partition_path = os.path.join(partition_path, f"{level}={value:02d}")

                if level == self.partition_level:
                    break

            partitions[partition_path].append(message)
            

        for partition_path, group_messages in partitions.items():
            now = pendulum.now("Asia/Ho_Chi_Minh")
            suffix  = uuid.uuid4().hex[:6]
            file_name = f"{self.kafka_topic}_{suffix}_{now.strftime('%Y%m%d_%H%M')}.jsonl.gz"
            full_path = f"{partition_path}/{file_name}"

            # Format and compress
            jsonl_data = "\n".join(json.dumps(msg) for msg in group_messages)
            buffer = BytesIO()
            with gzip.GzipFile(fileobj=buffer, mode='w') as f:
                f.write(jsonl_data.encode('utf-8'))

            # Upload
            bucket = self.gcs_client.bucket(self.bucket_name)
            blob = bucket.blob(full_path)
            blob.upload_from_string(buffer.getvalue(), content_type='application/gzip')

            self.log.info(f"Uploaded {len(group_messages)} messages to partition GCS at gs://{self.bucket_name}/{full_path}")
    

    def execute(self, context):
        messages = self.__consume_kafka_message()
        self.log.info(f"Consumed {len(messages)} messages from Kafka topic {self.kafka_topic}")
        self.__write_to_gcs(messages)