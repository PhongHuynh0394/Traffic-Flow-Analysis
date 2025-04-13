from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from hooks.redis_hook import RedisHook
from airflow.models.param import Param
from datetime import datetime
# import pendulum
import logging

PSQL_TABLE = "tbl__raw__dim_weather_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"
REDIS_CONN_ID = "conn_redis"

params = {
    "district": Param(
                type="string",
                default="Quận 1",
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
    'trans__weather_event',  
    description='Crawling weather event',
    schedule_interval="* * * * *",  
    start_date=datetime(2023, 4, 5),  
    catchup=False,
    params=params

) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    @task(provide_context=True)
    def get_weather_url(city: str = "Hồ Chí Minh", district: str = 'Quận 1'):
        from utils.preprocess import standard_location

        key = f"weather:{standard_location(city)}:{standard_location(district)}"
        redis_hook = RedisHook(REDIS_CONN_ID)
        data = redis_hook.get(key)

        if not data:
            hook = PostgresHook(postgres_conn_id=PSQL_CONN_ID)
            query = f"SELECT district_url FROM {PSQL_TABLE} WHERE district = '{district}' AND city = '{city}'"
            data = hook.get_records(query)[0][0]

            # cache
            redis_hook.set(key, data)

        return data


    @task(provide_context=True, multiple_outputs=True)
    def weather_crawling(url: str):
        from utils.crawling import WeatherCrawler

        crawler = WeatherCrawler()
        data = crawler.crawl(url)
        logging.info(data)

        # send to kafka
        return data
        

    end_task = DummyOperator(
        task_id='end'
    )

    weather_url = get_weather_url(district="{{params.district}}", city="{{params.city}}")
    weather_crawling_task = weather_crawling(weather_url)

    start_task >> weather_url >> weather_crawling_task >> end_task
