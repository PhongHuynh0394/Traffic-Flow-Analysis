from airflow.hooks.base_hook import BaseHook
from typing import Any, Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import json


class KafkaProducerHook(BaseHook):
    def __init__(self, config: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.client = Producer(self.config)
        self.admin_client = AdminClient(self.config)


    def _create_topic(self, topic_name: str):
        num_partitions = self.config.get("num_partitions", 1) 
        replication_factor = self.config.get("replication_factor", 1)

        topic_list = self.admin_client.list_topics().topics

        if topic_name in topic_list:
            self.log.info(f"Topic {topic_name} already exists.")
            return

        new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        try:
            self.admin_client.create_topics([new_topic])
            self.log.info(f"Topic {topic_name} created.")
        except KafkaException as e:
            self.log.error(f"Failed to create topic {topic_name}: {e}")


    def produce(self, topic: str, value: Any, key: Optional[str] = None, isflush: bool = True):
        self._create_topic(topic)
        self.client.produce(topic, key=key, value=json.dumps(value), callback=self.deliver)
        if isflush:
            self.close()

    
    def deliver(self, err, msg):
        if err is not None:
            self.log.error(f"Message delivery failed: {err}")
        else:
            self.log.info(f"Message delivered to T_{msg.topic()} P_[{msg.partition()}] O_{msg.offset()} Successfully")

    def close(self):
        self.client.flush()