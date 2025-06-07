CREATE TABLE traffic_flow.kafka_traffic_weather (
    cam_id String,
    district String,
    image_w Int32,
    image_h Int32,
    object_name String,
    object_confidence Float32,
    object_bbox Array(Float64),
    hour UInt8,
    dt Date,
    minute UInt8,

    ts DateTime,
    cloud_ceiling_m UInt16,
    cloud_cover_pct UInt8,
    dew_point_c Int16,
    humidity_pct UInt8,
    uv_index UInt8,
    uv_index_status String,
    pressure_mb UInt16,
    realfeel_c Int16,
    realfeel_shade_c Int16,
    temp_c Int16,
    visibility_km UInt8,
    status String,
    wind_kmph UInt8,
    wind_direction String,
    wind_gust_kmph UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker-1:9094',
    kafka_topic_list = 'traffic-weather-cleaned',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;


CREATE TABLE traffic_flow.rmt_traffic_weather_event (
    cam_id String,
    district String,
    image_w Int32,
    image_h Int32,
    object_name String,
    object_confidence Float32,
    object_bbox Array(Float64),
    hour UInt8,
    dt Date,
    minute UInt8,

    ts DateTime,
    cloud_ceiling_m UInt16,
    cloud_cover_pct UInt8,
    dew_point_c Int16,
    humidity_pct UInt8,
    uv_index UInt8,
    uv_index_status String,
    pressure_mb UInt16,
    realfeel_c Int16,
    realfeel_shade_c Int16,
    temp_c Int16,
    visibility_km UInt8,
    status String,
    wind_kmph UInt8,
    wind_direction String,
    wind_gust_kmph UInt8
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (cam_id, ts)
TTL ts + INTERVAL 7 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_flow.mv_traffic_weather_event
TO traffic_flow.rmt_traffic_weather_event
AS SELECT * FROM traffic_flow.kafka_traffic_weather;