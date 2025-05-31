from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.param import Param
from hooks.gcs_hook import GCSHook

import pendulum
import logging

from utils.spark.spark_io import SparkIO
from pyspark import SparkConf


GCS_BUCKET = "traffic_flow_thesis"
GCS_RAW_PATH = "raw"
GCS_STAGING_PATH = "staging"
GCS_CLEANED_PATH = "cleaned"

BATCH_PATTERN = "year={year}/month={month:02d}/day={day:02d}"

params={
    "run_date": Param(
        type=["string"],
        default=pendulum.now(tz="Asia/Ho_Chi_Minh").subtract(days=1).format("YYYY-MM-DD"),
        examples=["2025-01-10"],
        description="Run date, format YYYY-MM-DD",
        format="date"
    )
}

default_args = {
    "owner": "PhongHuynh0394",
    "depends_on_past": False,
    "retries": 3
}

packages = [
    "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"
]

spark_conf = (SparkConf()
                .set("spark.executor.memory", "4g")
                # .set("spark.sql.repl.eagerEval.enabled", True)
                .set("spark.jars", ",".join(packages))
                .setMaster("local[*]")
)


@task
def trans__staging__traffic(**context):
    from pyspark.sql import types as T, functions as F

    run_date = context['params'].get("run_date")

    if run_date is not None:
        try:
            run_date = pendulum.parse(run_date)
        except Exception as e:
            raise ValueError(f"Invalid date format or value: {run_date}") from e
    else:
        run_date = context.get("data_interval_start")

    batch = BATCH_PATTERN.format(year=run_date.year, month=run_date.month, day=run_date.day)
    # gs_path = f"gs://{GCS_BUCKET}/{GCS_RAW_PATH}/raw_event/traffic-object-raw/{batch}"
    gs_path = f"gs://{GCS_BUCKET}/raw_event/traffic-object-raw/{batch}"

    conf = SparkConf().setAll(spark_conf.getAll()).setAppName(context['task'].task_id)
    with SparkIO(conf=conf, gcs=True) as spark:

        # Traffic Schema
        object_schema = T.StructType([
            T.StructField("class_id", T.IntegerType()),
            T.StructField("class_object",T.StringType()),
            T.StructField("classname", T.StringType()),
            T.StructField("confidence", T.DoubleType()),
            T.StructField("coordinates", T.ArrayType(T.DoubleType()))
        ])

        traffic_schema = T.StructType([
            T.StructField("cam_id", T.StringType()),
            T.StructField("img", T.StringType()),
            T.StructField("district", T.StringType()),
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("image_shape", T.ArrayType(T.IntegerType())),
            T.StructField("total", T.IntegerType()),
            T.StructField("objects", T.ArrayType(object_schema)),
        ])

        try:
            logging.info(f"Read data batch: {batch} from GCS")
            traffic_df = (spark.read
                .schema(traffic_schema)
                .option("multiline", True)
                .json(gs_path)
            )
        except Exception as e:
            raise Exception(f"Fail to read data from GCS: {gs_path}, Error {e}")

        def cleaning_raw(df):

            # Cast type
            trans_df = (df
                    .withColumn("cam_id", F.trim(F.col("cam_id")).alias("cam_id"))
                    .withColumn("ts", F.col("timestamp").cast("timestamp"))
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
                        .withColumn("hour_of_day", F.hour(F.col("ts"))) \
                        .withColumn("dt", F.to_date("ts"))

            cols = ["cam_id", "district", "image_w", "image_h",
                        "object_name", "object_confidence",
                        "object_bbox", "ts", "hour_of_day", 'dt']

            return trans_df.select(*cols)
        

        logging.info("Start cleaning data")
        trans_df = cleaning_raw(traffic_df)
        # Save back to GCS staging
        staging_path = f"gs://{GCS_BUCKET}/{GCS_STAGING_PATH}/traffic-object-stag/{batch}"

        try:
            logging.info(f"Write result to {staging_path}")
            trans_df.write.mode("overwrite").parquet(staging_path)
        except Exception as e:
            raise Exception(f"Fail to write data to GCS: {staging_path}, Error {e}")

        return staging_path


@task
def trans__staging__weather(**context):
    from pyspark.sql import functions as F

    run_date = context['params'].get("run_date")

    if run_date is not None:
        try:
            run_date = pendulum.parse(run_date)
        except Exception as e:
            raise ValueError(f"Invalid date format or value: {run_date}") from e
    else:
        run_date = context.get("data_interval_start")

    batch = BATCH_PATTERN.format(year=run_date.year, month=run_date.month, day=run_date.day)
    # gs_path = f"gs://{GCS_BUCKET}/{GCS_RAW_PATH}/raw_event/weather-raw/{batch}"
    gs_path = f"gs://{GCS_BUCKET}/raw_event/weather-raw/{batch}"

    conf = SparkConf().setAll(spark_conf.getAll()).setAppName(context['task'].task_id)
    with SparkIO(conf=conf, gcs=True) as spark:
        try:
            logging.info(f"Read data batch: {batch} from GCS")
            weather_df = (spark.read
                .option("multiline", False)
                .json(gs_path)
            )
        except Exception as e:
            raise Exception(f"Fail to read data from GCS: {gs_path}, Error {e}")

    def transform_weather(df):

        def extract_number(col):
            return F.regexp_extract(col, r"(\d+)", 1).cast("int")

        def extract_uv_status(col):
            return F.lower(F.trim(F.regexp_extract(col, r"\d+\s*(\w+)", 1)))

        def extract_wind_direction(col):
            return F.regexp_extract(col, r"^([A-Z]+)", 1)

        trans_df = (
            df
            .withColumn("ts", F.col("timestamp").cast("timestamp"))
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
            .withColumn("hour_of_day", F.hour("ts"))
        )

        # Select relevant fields
        cols = [
            "ts", "district", "cloud_ceiling_m", "cloud_cover_pct", "dew_point_c",
            "humidity_pct", "uv_index", "uv_index_status", "pressure_mb", "realfeel_c",
            "realfeel_shade_c", "temp_c", "visibility_km", "status",
            "wind_kmph", "wind_direction", "wind_gust_kmph", "dt", "hour_of_day"
        ]

        return trans_df.select(*cols)
    
    logging.info("Start Cleaning data")
    trans_df = transform_weather(weather_df)
    # Save back to GCS staging
    staging_path = f"gs://{GCS_BUCKET}/{GCS_STAGING_PATH}/weather-stag/{batch}"
    try:
        logging.info(f"Write result to {staging_path}")
        trans_df.write.mode("overwrite").parquet(staging_path)
    except Exception as e:
        raise Exception(f"Error Writting result to GCS batch {batch}, error {e}")

    return staging_path


@task
def trans__cleaned__traffic_weather(weather_path, traffic_path, **context):
    run_date = context['params'].get("run_date")

    if run_date is not None:
        try:
            run_date = pendulum.parse(run_date)
        except Exception as e:
            raise ValueError(f"Invalid date format or value: {run_date}") from e
    else:
        run_date = context.get("data_interval_start")

    batch = BATCH_PATTERN.format(year=run_date.year, month=run_date.month, day=run_date.day)
    gs_path = f"gs://{GCS_BUCKET}/{GCS_CLEANED_PATH}/traffic-weather/{batch}"

    conf = SparkConf().setAll(spark_conf.getAll()).setAppName(context['task'].task_id)
    with SparkIO(conf=conf, gcs=True) as spark:
        weather_df = spark.read.parquet(weather_path)
        traffic_df = spark.read.parquet(traffic_path)

        weather_df = weather_df.drop("dt", "hour_of_day")
        joined_df = traffic_df.join(weather_df, on="ts", how="left")
        try:
            logging.info(f"Process successfully, write result to {gs_path}")
            joined_df.write.mode("overwrite").parquet(gs_path)
        except Exception as e:
            raise Exception(f"Error Writting result to GCS batch {batch}, error {e}")


with DAG(
    'trans_gcs_gcs__traffic_weather',
    description='Transform data in batch layer',
    default_args=default_args,
    schedule_interval="0 23 * * *",
    start_date=pendulum.datetime(2025, 5, 8, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=['batch', 'gcs', 'transform'],
    params=params,
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    traffic_staging = trans__staging__traffic()
    weather_staging = trans__staging__weather()
    cleaned_layer = trans__cleaned__traffic_weather(traffic_staging, weather_staging)

    end = DummyOperator(
        task_id='end'
    )

    start >> [traffic_staging, weather_staging] >> cleaned_layer >> end