-- Traffic event
CREATE TABLE traffic_flow.kafka_traffic (
    ts DateTime,
    cam_id String,
    hour UInt8,
    dt Date,
    minute UInt8,
    district String,
    image_w Nullable(Int32),
    image_h Nullable(Int32),
    object_name Nullable(String),
    object_confidence Nullable(Float32),
    object_bbox Array(Float64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker-1:9094',
    kafka_topic_list = 'traffic-weather-cleaned',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;


CREATE TABLE traffic_flow.rmt_traffic_event (
    ts DateTime,
    cam_id String,
    hour UInt8,
    dt Date,
    minute UInt8,
    district String,
    image_w Nullable(Int32),
    image_h Nullable(Int32),
    object_name Nullable(String),
    object_confidence Nullable(Float32),
    object_bbox Array(Float64)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (cam_id, ts)
TTL ts + INTERVAL 7 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_flow.mv_traffic_event
TO traffic_flow.rmt_traffic_event
AS SELECT * FROM traffic_flow.kafka_traffic;


-- weather event
CREATE TABLE traffic_flow.kafka_weather (
    ts DateTime,
    hour UInt8,
    dt Date,
    minute UInt8,
    district String,
    cloud_ceiling_m Nullable(UInt16),
    cloud_cover_pct Nullable(UInt8),
    dew_point_c Nullable(Int16),
    humidity_pct Nullable(UInt8),
    uv_index Nullable(UInt8),
    uv_index_status Nullable(String),
    pressure_mb Nullable(UInt16),
    realfeel_c Nullable(Int16),
    realfeel_shade_c Nullable(Int16),
    temp_c Nullable(Int16),
    visibility_km Nullable(UInt8),
    status Nullable(String),
    wind_kmph Nullable(UInt8),
    wind_direction Nullable(String),
    wind_gust_kmph Nullable(UInt8)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker-1:9094',
    kafka_topic_list = 'weather-cleaned',
    kafka_group_name = 'clickhouse_weather_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;


CREATE TABLE traffic_flow.rmt_weather_event (
    ts DateTime,
    hour UInt8,
    dt Date,
    minute UInt8,
    district String,
    cloud_ceiling_m Nullable(UInt16),
    cloud_cover_pct Nullable(UInt8),
    dew_point_c Nullable(Int16),
    humidity_pct Nullable(UInt8),
    uv_index Nullable(UInt8),
    uv_index_status Nullable(String),
    pressure_mb Nullable(UInt16),
    realfeel_c Nullable(Int16),
    realfeel_shade_c Nullable(Int16),
    temp_c Nullable(Int16),
    visibility_km Nullable(UInt8),
    status Nullable(String),
    wind_kmph Nullable(UInt8),
    wind_direction Nullable(String),
    wind_gust_kmph Nullable(UInt8)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (ts, district)
TTL ts + INTERVAL 7 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_flow.mv_weather_event
TO traffic_flow.rmt_weather_event
AS SELECT * FROM traffic_flow.kafka_weather;