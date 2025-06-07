import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from contextlib import contextmanager
import json 
import logging


# GCS connector jars
# "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"
# OR
# com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.9 (Maven)

# Kafka connector jars.packages
# "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4"

class SparkIO:
    def __init__(self, conf: SparkConf = SparkConf(), gcs: bool = True):
        self.app_name = conf.get("spark.app.name")
        self.master = conf.get("spark.master")
        self.conf = conf
        self.gcs = gcs
        self._spark = None
    
    def __enter__(self):

        spark = SparkSession.builder.config(conf=self.conf).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        logging.info(f'Create SparkSession app {self.app_name} with {self.master} mode')
        if self.gcs:
            service_account_path = "/tmp/service_account.json"
            with open(service_account_path, "w") as f:
                json.dump(json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')), f)

            spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.json.keyfile", service_account_path)
            # spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
            # spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", gg_service_path)
        self._spark = spark

        return spark


    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.info(f'Stop SparkSession app {self.app_name}')
        if self._spark:
            self._spark.stop()
    

    def get_instance(self):
        return self._spark if self._spark else self.__enter__()
    

    def close(self):
        self.__exit__(None, None, None)
