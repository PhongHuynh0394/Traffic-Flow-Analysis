
from airflow import DAG
from airflow.decorators import task
import pendulum

from pyspark import SparkConf
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, regexp_replace, trim, regexp_extract, to_timestamp, from_json
from pyspark.sql.types import DoubleType

KAFKA_SERVER = "kafka-broker-1:9094"
KAFKA_TOPIC = "weather-raw"

default_args = {
    "owner": "PhongHuynh0394",
    "depends_on_past": False,
    "retries": 0
}

with DAG(
    'trans_kafka_druid__weather_event',  
    description='Spark Streaming transform Kafka weather event to Druid',
    default_args=default_args,
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 4, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False
) as dag:
    
    @task
    def transform_weather_event():
        from utils.spark.spark_io import SparkIO

        # Spark kafka
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4"
        ]

        conf = (SparkConf().setAppName("Weather-Kafka-Druid")
            .set("spark.executor.memory", "2g")
            # .set("spark.driver.memory", "2g")
            .set("spark.jars.packages", ",".join(packages))
            .setMaster("local[*]")
            )

        with SparkIO(conf=conf) as spark:

            stream_df = spark.readStream\
                .format("kafka")\
                .option("kafka.bootstrap.servers", KAFKA_SERVER)\
                .option("subscribe", KAFKA_TOPIC)\
                .load()
            
            stream_df = stream_df.selectExpr("CAST(value AS STRING)")

            schema = StructType() \
                .add("current_time", StringType()) \
                .add("status", StringType()) \
                .add("temp_c", StringType()) \
                .add("realfeel®", StringType()) \
                .add("wind", StringType()) \
                .add("wind gusts", StringType()) \
                .add("humidity", StringType()) \
                .add("indoor humidity", StringType()) \
                .add("dew point", StringType()) \
                .add("pressure", StringType()) \
                .add("cloud cover", StringType()) \
                .add("visibility", StringType()) \
                .add("cloud ceiling", StringType()) \
                .add("timestamp", StringType())
        
            parsed_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

            # Clean and standardize
            cleaned_df = parsed_df \
                .withColumn("temp_c", regexp_extract(col("temp_c"), r"([\d.]+)", 1).cast(DoubleType())) \
                .withColumn("realfeel", regexp_extract(col("realfeel®"), r"([\d.]+)", 1).cast(DoubleType())) \
                .withColumn("wind_kmh", regexp_extract(col("wind"), r"(\d+)", 1).cast(DoubleType())) \
                .withColumn("wind_gusts_kmh", regexp_extract(col("wind gusts"), r"(\d+)", 1).cast(DoubleType())) \
                .withColumn("humidity", regexp_replace(col("humidity"), "%", "").cast(DoubleType())) \
                .withColumn("indoor_humidity", regexp_extract(col("indoor humidity"), r"(\d+)", 1).cast(DoubleType())) \
                .withColumn("dew_point_c", regexp_extract(col("dew point"), r"([\d.]+)", 1).cast(DoubleType())) \
                .withColumn("pressure_mb", regexp_extract(col("pressure"), r"(\d+)", 1).cast(DoubleType())) \
                .withColumn("cloud_cover", regexp_replace(col("cloud cover"), "%", "").cast(DoubleType())) \
                .withColumn("visibility_km", regexp_extract(col("visibility"), r"([\d.]+)", 1).cast(DoubleType())) \
                .withColumn("cloud_ceiling_m", regexp_extract(col("cloud ceiling"), r"(\d+)", 1).cast(DoubleType())) \
                .withColumn("timestamp", to_timestamp("timestamp"))  # Druid likes proper timestamps

            # Only get cleaned columns
            final_df = cleaned_df.select(
                "timestamp",
                "temp_c",
                "realfeel",
                "wind_kmh",
                "wind_gusts_kmh",
                "humidity",
                "indoor_humidity",
                "dew_point_c",
                "pressure_mb",
                "cloud_cover",
                "visibility_km",
                "cloud_ceiling_m"
            )

            query = final_df.writeStream\
                .outputMode("append")\
                .format("console")\
                .option("truncate", False)\
                .start()

            query.awaitTermination()
    

    transform_weather_event()