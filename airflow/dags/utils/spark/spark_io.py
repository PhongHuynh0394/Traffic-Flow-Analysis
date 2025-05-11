import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from contextlib import contextmanager
import json 
import logging


# GCS connector jars
# "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"

# Kafka connector jars.packages
# "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4"

@contextmanager
def SparkIO(conf: SparkConf = SparkConf(), 
            gcs: bool = False):
    app_name = conf.get("spark.app.name")
    master = conf.get("spark.master")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logging.info(f'Create SparkSession app {app_name} with {master} mode')
    try:
        if gcs:
            service_account_path = "/tmp/service_account.json"
            with open(service_account_path, "w") as f:
                json.dump(json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')), f)

            spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.json.keyfile", service_account_path)
            # spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
            # spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", gg_service_path)

        yield spark
    except Exception:
        raise Exception
    finally:
        if os.path.exists(service_account_path):
            os.remove(service_account_path)

        logging.info(f'Stop SparkSession app {app_name}')
        spark.stop()