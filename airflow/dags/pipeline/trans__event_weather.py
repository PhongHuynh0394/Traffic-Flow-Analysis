from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
# import pendulum
import logging

PSQL_TABLE = "tbl__raw__dim_weather_info"
PSQL_CONN_ID = "conn_psql__raw_crawl"
URL = [
    "https://www.accuweather.com/en/vn/district-1/3554433/current-weather/3554433"
]


# Initialize the DAG
with DAG(
    'trans__weather_event',  
    description='Crawling weather event',
    schedule_interval="* * * * *",  
    start_date=datetime(2023, 4, 5),  
    catchup=False
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )


    @task(provide_context=True, multiple_outputs=True)
    def weather_crawling(**kwargs):
        from utils.crawling import WeatherCrawler

        hook = PostgresHook(postgres_conn_id=PSQL_CONN_ID)
        query = f"SELECT district_url FROM {PSQL_TABLE} WHERE district = 'Quận 1' AND city = 'Hồ Chí Minh'"
        url = hook.get_records(query)[0][0]

        crawler = WeatherCrawler()
        data = crawler.crawl(url)
        logging.info(data)
        return data
        

    end_task = DummyOperator(
        task_id='end'
    )

    weather_crawling_task = weather_crawling()

    start_task >> weather_crawling_task >> end_task
