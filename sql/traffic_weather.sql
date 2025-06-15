CREATE TABLE traffic_flow.rmt_traffic_weather_joined (
    ts DateTime('Asia/Ho_Chi_Minh'),
    cam_id String,
    district String,
    image_w Nullable(Int32),
    image_h Nullable(Int32),
    object_name Nullable(String),
    object_confidence Nullable(Float32),
    object_bbox Array(Float64),

    -- From weather
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
    wind_gust_kmph Nullable(UInt8),

    dt Date,
    hour UInt8,
    minute UInt8
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (district, dt, hour, minute, cam_id)
TTL ts + INTERVAL 7 DAY;


CREATE MATERIALIZED VIEW traffic_flow.mv_traffic_weather_joined
TO traffic_flow.rmt_traffic_weather_joined
AS
SELECT
    t.ts,
    t.cam_id,
    t.district,
    t.image_w,
    t.image_h,
    t.object_name,
    t.object_confidence,
    t.object_bbox,

    w.cloud_ceiling_m,
    w.cloud_cover_pct,
    w.dew_point_c,
    w.humidity_pct,
    w.uv_index,
    w.uv_index_status,
    w.pressure_mb,
    w.realfeel_c,
    w.realfeel_shade_c,
    w.temp_c,
    w.visibility_km,
    w.status,
    w.wind_kmph,
    w.wind_direction,
    w.wind_gust_kmph,

    t.dt,
    t.hour,
    t.minute

FROM traffic_flow.rmt_traffic_event t
LEFT JOIN traffic_flow.rmt_weather_event w
ON t.district = w.district
   AND t.dt = w.dt
   AND t.hour = w.hour
   AND t.minute = w.minute;