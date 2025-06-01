from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from hooks.redis_hook import RedisHook
from hooks.kafka_hook import KafkaProducerHook
from airflow.models.param import Param
from airflow.models import Variable
from datetime import datetime
import pendulum
import logging

PSQL_TABLE = "tbl__raw__dim_weather_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"
REDIS_CONN_ID = "conn_redis"
KAFKA_TOPIC = "weather-raw"
# PROXY_TOKEN = Variable.get("token__proxy")
PROXY_TOKEN = None

kafka_config={
    "bootstrap.servers": "kafka-broker-1:9094",
    "replication_factor": 1,
    "num_partitions": 1,
}

default_args = {
    "owner": "PhongHuynh0394",
    "depends_on_past": False,
    "retries": 0
}

params = {
    "district": Param(
                type="array",
                default=["Quận 1", "Quận 3", "Quận 5", "Quận 10"],
                description="Weather of which district",
                examples=["Quận 1"]),
    "city": Param(
                type="string",
                default="Hồ Chí Minh",
                description="Weather of which city",
                examples=["Hồ Chí Minh"])
}


# Initialize the DAG
with DAG(
    'trans_ex_kafka__raw_weather',  
    description='Crawling weather event',
    default_args=default_args,
    schedule_interval="* * * * *",  
    start_date=pendulum.datetime(2023, 4, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    params=params,
    tags=["producer", "raw"]
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    @task(provide_context=True)
    def get_weather_url(city: str = "Hồ Chí Minh", district: str = "Quận 1"):
        from utils.preprocess import standard_location

        key = f"weather:{standard_location(city)}:{standard_location(district)}"
        redis_hook = RedisHook(REDIS_CONN_ID)
        data = redis_hook.get(key)

        if not data:
            hook = PostgresHook(postgres_conn_id=PSQL_CONN_ID)
            query = f"SELECT district_url FROM {PSQL_TABLE} WHERE district = '{district}' AND city = '{city}'"
            data = hook.get_records(query)[0][0]

            # cache
            redis_hook.set(key, data, ttl=300)

        return data


    @task(provide_context=True, multiple_outputs=True)
    def weather_crawling(url: str, district: str):
        from utils.crawling import WeatherCrawler

        crawler = WeatherCrawler(proxy_token=PROXY_TOKEN)
        data = crawler.crawl(url)
        logging.info(data)

        now = pendulum.now("Asia/Ho_Chi_Minh")
        # timestamp_str = now.to_datetime_string() # 'YYYY-MM-DD HH:MM:SS'
        timestamp_str = now.to_iso8601_string()  # e.g. '2025-06-01T20:29:07+07:00'
        data.update({
            "timestamp": timestamp_str,
            "district": district
        })

        # send to kafka
        kafka_hook = KafkaProducerHook(config=kafka_config)
        kafka_hook.produce(
            topic=KAFKA_TOPIC,
            value=data,
            isflush=True
        )
        return data
        

    end_task = DummyOperator(
        task_id='end'
    )


    for district in dag.params["district"]:
        district_name = district.replace('Quận ', 'district_')
        weather_url = get_weather_url.override(
            task_id=f"get_url__{district_name}")(district=district, city="{{params.city}}")

        weather_crawling_task = weather_crawling.override(
            task_id=f"crawling__{district_name}")(url=weather_url, district=district)
            

        start_task >> weather_url >> weather_crawling_task >> end_task
