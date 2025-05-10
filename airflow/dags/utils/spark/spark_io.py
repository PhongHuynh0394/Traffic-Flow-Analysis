from pyspark.sql import SparkSession
from pyspark import SparkConf
from contextlib import contextmanager
import logging


@contextmanager
def SparkIO(conf: SparkConf = SparkConf()):
    app_name = conf.get("spark.app.name")
    master = conf.get("spark.master")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logging.info(f'Create SparkSession app {app_name} with {master} mode')
    try:
        yield spark
    except Exception:
        raise Exception
    finally:
        print(f'Stop SparkSession app {app_name}')
        spark.stop()