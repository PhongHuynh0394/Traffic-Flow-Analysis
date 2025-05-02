# from airflow.hooks.base_hook import BaseHook
from airflow.hooks.base import BaseHook
from typing import Any, Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import json


class KafkaProducerHook(BaseHook):
    def __init__(self, config: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.replication_factor = config.pop("replication_factor", 1)
        self.num_partitions = config.pop("num_partitions", 1)
        self.config = config
        self.client = Producer(self.config)
        self.admin_client = AdminClient(self.config)


    def _create_topic(self, topic_name: str):

        topic_list = self.admin_client.list_topics().topics

        if topic_name in topic_list:
            self.log.info(f"Topic {topic_name} already exists.")
            return

        new_topic = NewTopic(topic_name, num_partitions=self.num_partitions, replication_factor=self.replication_factor)
        fs = self.admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                self.log.info(f"Topic {topic} created.")
            except KafkaException as e:
                self.log.error(f"Failed to create topic {topic}: {e}")


    def produce(self, topic: str, value: Any, key: Optional[str] = None, isflush: bool = True):
        self._create_topic(topic)
        self.client.produce(topic, key=key, value=json.dumps(value), callback=self.deliver)
        self.client.poll(0)
        if isflush:
            self.close()

    
    def deliver(self, err, msg):
        if err is not None:
            self.log.error(f"Message delivery failed: {err}")
        else:
            self.log.info(f"Message delivered to T_{msg.topic()} P_[{msg.partition()}] O_{msg.offset()} Successfully")

    def close(self):
        self.client.flush()