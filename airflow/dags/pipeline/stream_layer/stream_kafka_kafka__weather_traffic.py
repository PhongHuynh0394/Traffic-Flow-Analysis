from typing import Optional, Literal
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from pyspark import SparkConf
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType, StructField, ArrayType
from pyspark.sql.functions import col, regexp_replace, trim, regexp_extract, to_timestamp, from_json
from pyspark.sql import functions as F, types as T

from utils.spark.spark_io import SparkIO

KAFKA_SERVER = "kafka-broker-1:9092"
KAFKA_SOURCE_WEATHER = "weather-raw"
KAFKA_SOURCE_TRAFFIC = "traffic-object-raw"
KAFKA_SINK_TOPIC = "traffic-weather-cleaned"


default_args = {
    "owner": "PhongHuynh0394",
    "depends_on_past": False,
    "retries": 3
}

# Spark kafka connector
packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4"
]

weather_conf = (SparkConf().setAppName("Raw-Weather-Processing")
    .set("spark.executor.memory", "512m")
    .set("spark.driver.memory", "512m") 
    .set("spark.cores.max", "1")
    .set("spark.jars.packages", ",".join(packages))
    .set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    .setMaster("local[*]")
    # .setMaster("spark://spark-master:7077")
    )

traffic_conf = (SparkConf().setAppName("Raw-Traffic-Processing")
    .set("spark.executor.memory", "512m")
    .set("spark.driver.memory", "512m") 
    .set("spark.cores.max", "1")
    .set("spark.jars.packages", ",".join(packages))
    .set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    .setMaster("local[*]")
    # .setMaster("spark://spark-master:7077")
    )

def read_kafka_stream(spark, topic, schema):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json("json", schema).alias("data"))
            .select("data.*"))


def write_kafka_stream(df, topic, key: Optional[str] = None):
    # Serialize to key value format
    result = df.withColumn("value", F.to_json(F.struct(*df.columns)))
    result = result.withColumn("key", F.lit(key).cast("string")) if key is None else result.withColumn("key", F.col(key).cast("string"))

    # Write to kafka topic
    result.select("key", "value").writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("topic", topic) \
        .option("checkpointLocation", f"/tmp/kafka_checkpoint_{topic}") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


with DAG(
    'trans_kafka_kafka__traffic_weather_processing',  
    description='Spark Streaming transform weather and traffic event from Kafka',
    default_args=default_args,
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 4, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False
) as dag:

    @task
    def process__weather_event():
        weather_schema = StructType([
            StructField("cloud ceiling", StringType(), True),
            StructField("cloud cover", StringType(), True),
            StructField("current_time", StringType(), True),
            StructField("dew point", StringType(), True),
            StructField("humidity", StringType(), True),
            StructField("indoor humidity", StringType(), True),
            StructField("max uv index", StringType(), True),
            StructField("pressure", StringType(), True),
            StructField("realfeel shade™", StringType(), True),
            StructField("realfeel®", StringType(), True),
            StructField("status", StringType(), True),
            StructField("temp_c", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("visibility", StringType(), True),
            StructField("wind", StringType(), True),
            StructField("wind gusts", StringType(), True),
            StructField("day", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("district", StringType())
        ])

        def transform_weather(df):
            
            def extract_number(col):
                return F.regexp_extract(col, r"(\d+)", 1).cast("int")

            def extract_uv_status(col):
                return F.lower(F.trim(F.regexp_extract(col, r"\d+\s*(\w+)", 1)))

            def extract_wind_direction(col):
                return F.regexp_extract(col, r"^([A-Z]+)", 1)

            trans_df = (
                df
                # .withColumn("ts", F.col("timestamp").cast("string"))
                .withColumn("ts", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
                .withColumn("cloud_ceiling_m", extract_number("cloud ceiling"))
                .withColumn("cloud_cover_pct", extract_number("cloud cover"))
                .withColumn("dew_point_c", extract_number("dew point"))
                .withColumn("humidity_pct", extract_number("humidity"))
                .withColumn("uv_index", extract_number("max uv index"))
                .withColumn("uv_index_status", extract_uv_status("max uv index"))
                .withColumn("pressure_mb", extract_number("pressure"))
                .withColumn("realfeel_c", extract_number("realfeel®"))
                .withColumn("realfeel_shade_c", extract_number("realfeel shade™"))
                .withColumn("temp_c", extract_number("temp_c"))
                .withColumn("visibility_km", extract_number("visibility"))
                .withColumn("wind_kmph", extract_number("wind"))
                .withColumn("wind_direction", extract_wind_direction("wind"))
                .withColumn("wind_gust_kmph", extract_number("wind gusts"))
                .withColumn("status", F.lower(F.col("status")))
                .withColumn("dt", F.to_date("ts"))
                .withColumn("hour", F.hour("ts"))
                .withColumn("minute", F.minute("ts"))
            )

            # Select relevant fields
            cols = [
                "ts", "district", "cloud_ceiling_m", "cloud_cover_pct", "dew_point_c",
                "humidity_pct", "uv_index", "uv_index_status", "pressure_mb", "realfeel_c",
                "realfeel_shade_c", "temp_c", "visibility_km", "status",
                "wind_kmph", "wind_direction", "wind_gust_kmph", "dt", "hour", "minute"
            ]

            return trans_df.select(*cols)

        with SparkIO(conf=weather_conf) as spark:

            weather_raw = read_kafka_stream(spark, KAFKA_SOURCE_WEATHER, weather_schema)
            weather_clean = transform_weather(weather_raw)
    
            write_kafka_stream(weather_clean, "weather-cleaned", key=None)


    @task
    def process__traffic_event():

        object_schema = StructType([
            StructField("class_id", IntegerType()),
            StructField("class_object", StringType()),
            StructField("classname", StringType()),
            StructField("confidence", DoubleType()),
            StructField("coordinates", ArrayType(DoubleType()))
        ])

        traffic_schema = StructType([
            StructField("cam_id", StringType()),
            StructField("district", StringType()),
            StructField("img", StringType()),
            StructField("timestamp", StringType()),
            StructField("image_shape", ArrayType(IntegerType())),
            StructField("total", IntegerType()),
            StructField("objects", ArrayType(object_schema)),
        ])

        def transform_traffic(df):
            
            # Cast type
            trans_df = (df
                    .withColumn("cam_id", F.trim(F.col("cam_id")).alias("cam_id"))
                    # .withColumn("ts", F.col("timestamp").cast("timestamp"))
                    .withColumn("ts", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
                    .withColumn("image_h", F.element_at("image_shape", 1).cast("int"))
                    .withColumn("image_w", F.element_at("image_shape", 2).cast("int"))
            )
            
            # Explode dict object
            trans_df = trans_df.withColumn("obj", F.explode_outer("objects"))
            trans_df = (trans_df
                    .withColumn("object_id", F.col("obj.class_id").cast("int"))
                    .withColumn("object_name", F.col("obj.class_object").cast("string"))
                    .withColumn("object_confidence",
                                F.when(F.col("obj.confidence").between(0.0, 1.0),
                                    F.col("obj.confidence")))
                    .withColumn("object_bbox",
                                F.when(F.size("obj.coordinates") == 4,
                                    F.col("obj.coordinates").cast("array<double>")))
            )
    
            trans_df = trans_df.filter(
                    (F.col("ts").isNotNull())
                )
    
            trans_df = trans_df \
                        .withColumn("hour", F.hour(F.col("ts"))) \
                        .withColumn("dt", F.to_date("ts")) \
                        .withColumn("minute", F.minute("ts"))
    
            cols = ["cam_id", "ts", "total", "district", "image_w", "image_h",
                        "object_name", "object_confidence",
                        "object_bbox", "hour", 'dt', "minute"]
    
            return trans_df.select(*cols)


        with SparkIO(conf=traffic_conf) as spark:
            traffic_raw = read_kafka_stream(spark, KAFKA_SOURCE_TRAFFIC, traffic_schema)
            traffic_clean = transform_traffic(traffic_raw)
    
            # Apply watermarks
            # traffic_clean = traffic_clean.withWatermark("ts", "2 minutes")
            # weather_clean = weather_clean.withWatermark("ts", "2 minutes")

            # # Join on district, dt, hour, minute
            # joined_df = traffic_clean.alias("t").join(
            #     weather_clean.alias("w"),
            #     on=[
            #         F.col("t.district") == F.col("w.district"),
            #         F.col("t.dt") == F.col("w.dt"),
            #         F.col("t.hour") == F.col("w.hour"),
            #         F.col("t.minute") == F.col("w.minute")
            #     ],
            #     how="left"
            # )

            write_kafka_stream(traffic_clean, "traffic-cleaned", key=None)


    process_traffic = process__traffic_event()
    process_weather = process__weather_event()