from confluent_kafka import Producer
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException


def create_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = AdminClient(conf)
    topic_list = admin_client.list_topics().topics

    if topic_name in topic_list:
        print(f"Topic {topic_name} already exists.")
        return

    new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    try:
        admin_client.create_topics([new_topic])
        print(f"Topic {topic_name} created.")
    except KafkaException as e:
        print(f"Failed to create topic {topic_name}: {e}")


if __name__ == "__main__":
    broker = "localhost:9092"
    conf = {
        "bootstrap.servers": broker
    }
    topic = "test-topic"
    create_topic(topic)
    producer = Producer(conf)

    index = 0
    while True:
        message = f"message {index}"
        producer.produce(topic, value=message)
        print(f"Produced message: {message}")
        index += 1
        time.sleep(3)