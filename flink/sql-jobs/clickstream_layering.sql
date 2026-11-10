-- Flink 运行参数
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.attached-execution' = 'false';
SET 'state.backend' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '10s';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'parallelism.default' = '1';
SET 'table.local-time-zone' = 'UTC';

-- 加载 connector JAR
ADD JAR '/opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar';
ADD JAR '/opt/flink/lib/flink-json-1.18.1.jar';
ADD JAR '/opt/flink/lib/iceberg-flink-runtime-1.18-1.5.0.jar';
ADD JAR '/opt/flink/lib/hadoop-common-2.8.3.jar';
ADD JAR '/opt/flink/lib/hadoop-hdfs-2.8.3.jar';
ADD JAR '/opt/flink/lib/hadoop-client-2.8.3.jar';
ADD JAR '/opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar';
ADD JAR '/opt/flink/lib/bundle-2.20.18.jar';

SHOW JARS;

-- Catalog/数据库：参数解析自 trino/iceberg.properties
DROP CATALOG IF EXISTS iceberg;

CREATE CATALOG iceberg WITH (

    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
    'uri' = 'http://rest:8181',
    'warehouse' = 's3://warehouse/',
    's3.endpoint' = 'http://minio:9000',
    'client.region' = 'us-east-1',
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'password',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'

);

CREATE DATABASE IF NOT EXISTS iceberg.ods;

CREATE DATABASE IF NOT EXISTS iceberg.dwd;

CREATE DATABASE IF NOT EXISTS iceberg.dim;

-- Kafka source：原始字符串入场，event_time 先判定后转换，watermark 在隔离列之后
DROP TABLE IF EXISTS clickstream_source;

CREATE TABLE clickstream_source (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    url STRING,
    session_id STRING,
    device STRING,
    event_time STRING,
    geo_location ROW<lat DOUBLE, lon DOUBLE>,
    purchase_amount DOUBLE,
    -- Kafka 原始消息（connector 的 value metadata 固定为 BYTES）：
    -- 脏记录隔离时 CAST 成 STRING 原样留存到 dirty.raw_payload，比事后重建 JSON 忠实
    raw_value BYTES METADATA FROM 'value' VIRTUAL,
    -- 入场判定：四类脏数据，顺序与 layering.contract.REJECT_REASONS 一致
    required_missing BOOLEAN AS (COALESCE(TRIM(event_id), '') = '' OR COALESCE(TRIM(user_id), '') = '' OR COALESCE(TRIM(event_type), '') = '' OR COALESCE(TRIM(url), '') = '' OR COALESCE(TRIM(session_id), '') = '' OR COALESCE(TRIM(device), '') = '' OR COALESCE(TRIM(event_time), '') = ''),
    event_time_valid BOOLEAN AS (event_time IS NOT NULL AND event_time REGEXP '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z$' AND UNIX_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss''Z''') IS NOT NULL),
    event_type_valid BOOLEAN AS (event_type IS NOT NULL AND event_type IN ('page_view', 'add_to_cart', 'purchase', 'logout')),
    geo_valid BOOLEAN AS (geo_location IS NOT NULL AND geo_location.lat IS NOT NULL AND geo_location.lon IS NOT NULL AND geo_location.lat BETWEEN -90.0 AND 90.0 AND geo_location.lon BETWEEN -180.0 AND 180.0),
    -- 坏时间换算成 NULL 而不是抛异常：TO_TIMESTAMP_LTZ 只在正则放行后才调用
    event_time_calc TIMESTAMP_LTZ(3) AS (
        CASE WHEN (event_time IS NOT NULL AND event_time REGEXP '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z$' AND UNIX_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss''Z''') IS NOT NULL)
             THEN TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss''Z'''), 0)
             ELSE CAST(NULL AS TIMESTAMP_LTZ(3))
        END
    ),
    reject_reason STRING AS (
        CASE
            WHEN (COALESCE(TRIM(event_id), '') = '' OR COALESCE(TRIM(user_id), '') = '' OR COALESCE(TRIM(event_type), '') = '' OR COALESCE(TRIM(url), '') = '' OR COALESCE(TRIM(session_id), '') = '' OR COALESCE(TRIM(device), '') = '' OR COALESCE(TRIM(event_time), '') = '') THEN 'MISSING_REQUIRED_FIELD'
            WHEN NOT (event_time IS NOT NULL AND event_time REGEXP '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z$' AND UNIX_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss''Z''') IS NOT NULL) THEN 'INVALID_EVENT_TIME'
            WHEN event_type IS NULL OR event_type NOT IN ('page_view', 'add_to_cart', 'purchase', 'logout')
                THEN 'INVALID_EVENT_TYPE'
            WHEN NOT (geo_location IS NOT NULL AND geo_location.lat IS NOT NULL AND geo_location.lon IS NOT NULL AND geo_location.lat BETWEEN -90.0 AND 90.0 AND geo_location.lon BETWEEN -180.0 AND 180.0) THEN 'INVALID_GEO_LOCATION'
            ELSE CAST(NULL AS STRING)
        END
    ),
    -- watermark 必须定义在所有判定/隔离列之后：坏记录 event_time_calc 为 NULL，
    -- 不参与 watermark 推进，乱序脏数据拖不住 watermark，去重排序也不被污染
    WATERMARK FOR event_time_calc AS event_time_calc
        - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clickstream',
    'properties.bootstrap.servers' = 'broker:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 四张 Iceberg 表（schema 来自 layering.contract）
DROP TABLE IF EXISTS iceberg.ods.clickstream;
CREATE TABLE iceberg.ods.clickstream (
    event_id STRING COMMENT '事件唯一 ID',
    user_id STRING COMMENT '用户 ID',
    event_type STRING COMMENT '事件类型，白名单见契约',
    url STRING COMMENT '事件发生页面',
    session_id STRING COMMENT '会话 ID',
    device STRING COMMENT '设备类型',
    event_time TIMESTAMP(6) COMMENT '事件时间（UTC）',
    latitude DOUBLE COMMENT '纬度',
    longitude DOUBLE COMMENT '经度',
    purchase_amount DOUBLE COMMENT '消费金额，可空',
    ingested_at TIMESTAMP(6) COMMENT '入湖时间'
) WITH (
    'format' = 'parquet'
);

DROP TABLE IF EXISTS iceberg.ods.clickstream_dirty;
CREATE TABLE iceberg.ods.clickstream_dirty (
    event_id STRING COMMENT '事件唯一 ID',
    user_id STRING COMMENT '用户 ID',
    event_type STRING COMMENT '事件类型，白名单见契约',
    url STRING COMMENT '事件发生页面',
    session_id STRING COMMENT '会话 ID',
    device STRING COMMENT '设备类型',
    event_time TIMESTAMP(6) COMMENT '事件时间（UTC）',
    latitude DOUBLE COMMENT '纬度',
    longitude DOUBLE COMMENT '经度',
    purchase_amount DOUBLE COMMENT '消费金额，可空',
    reject_reason STRING COMMENT '脏数据判定原因（四类之一）',
    raw_payload STRING COMMENT '原始 Kafka 消息 JSON',
    rejected_at TIMESTAMP(6) COMMENT '隔离时间'
) WITH (
    'format' = 'parquet'
);

DROP TABLE IF EXISTS iceberg.dwd.clickstream_dedup;
CREATE TABLE iceberg.dwd.clickstream_dedup (
    event_id STRING COMMENT '事件唯一 ID',
    user_id STRING COMMENT '用户 ID',
    event_type STRING COMMENT '事件类型，白名单见契约',
    url STRING COMMENT '事件发生页面',
    session_id STRING COMMENT '会话 ID',
    device STRING COMMENT '设备类型',
    event_time TIMESTAMP(6) COMMENT '事件时间（UTC）',
    latitude DOUBLE COMMENT '纬度',
    longitude DOUBLE COMMENT '经度',
    purchase_amount DOUBLE COMMENT '消费金额，可空',
    processed_at TIMESTAMP(6) COMMENT '处理时间',
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'format' = 'parquet',
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

DROP TABLE IF EXISTS iceberg.dim.dim_user;
CREATE TABLE iceberg.dim.dim_user (
    user_id STRING COMMENT '用户 ID（主键）',
    event_type STRING COMMENT '最近一次事件类型',
    url STRING COMMENT '最近一次访问页面',
    session_id STRING COMMENT '最近一次会话 ID',
    device STRING COMMENT '最近一次设备类型',
    event_time TIMESTAMP(6) COMMENT '最近一次事件时间',
    latitude DOUBLE COMMENT '最近一次纬度',
    longitude DOUBLE COMMENT '最近一次经度',
    purchase_amount DOUBLE COMMENT '最近一次消费金额',
    updated_at TIMESTAMP(6) COMMENT 'profile 更新时间',
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'format' = 'parquet',
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

-- 分流视图
CREATE TEMPORARY VIEW clickstream_dirty_view AS
SELECT
    event_id, user_id, event_type, url, session_id, device,
    CAST(event_time_calc AS TIMESTAMP(6)) AS event_time,
    geo_location.lat AS latitude,
    geo_location.lon AS longitude,
    purchase_amount,
    reject_reason,
    CAST(raw_value AS STRING) AS raw_payload,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS rejected_at
FROM clickstream_source
WHERE reject_reason IS NOT NULL;

CREATE TEMPORARY VIEW clickstream_clean_view AS
SELECT
    event_id, user_id, event_type, url, session_id, device,
    CAST(event_time_calc AS TIMESTAMP(6)) AS event_time,
    event_time_calc AS rowtime_event_time,
    geo_location.lat AS latitude,
    geo_location.lon AS longitude,
    purchase_amount
FROM clickstream_source
WHERE reject_reason IS NULL;

-- 四个 sink
-- ODS 原样层：source 全量 append-only 写入（坏 event_time 落 NULL，原始 JSON 在 dirty 表留存）
INSERT INTO iceberg.ods.clickstream (event_id, user_id, event_type, url, session_id, device, event_time, latitude, longitude, purchase_amount, ingested_at)
SELECT
    event_id, user_id, event_type, url, session_id, device,
    CAST(event_time_calc AS TIMESTAMP(6)),
    geo_location.lat, geo_location.lon, purchase_amount,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))
FROM clickstream_source;

-- 脏数据隔离层：带 reject_reason，禁止静默丢弃
INSERT INTO iceberg.ods.clickstream_dirty (event_id, user_id, event_type, url, session_id, device, event_time, latitude, longitude, purchase_amount, reject_reason, raw_payload, rejected_at)
SELECT
    event_id, user_id, event_type, url, session_id, device,
    event_time, latitude, longitude, purchase_amount,
    reject_reason, raw_payload, rejected_at
FROM clickstream_dirty_view;

-- DWD 明细层：合格流按 event_id 去重（首条生效），Iceberg v2 UPSERT
-- 规范 DEDUP 模式：ROW_NUMBER + 单一 rowtime 排序键，输出以 event_id 为唯一键的 upsert 流
INSERT INTO iceberg.dwd.clickstream_dedup (event_id, user_id, event_type, url, session_id, device, event_time, latitude, longitude, purchase_amount, processed_at)
SELECT
    event_id, user_id, event_type, url, session_id, device,
    event_time, latitude, longitude, purchase_amount,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY rowtime_event_time ASC
        ) AS rn
    FROM clickstream_clean_view
)
WHERE rn = 1;

-- DIM 用户层：按 user_id 取 event_time 最新的 profile，Iceberg v2 UPSERT
-- last-row DEDUP：rowtime 倒序 rn=1，输出以 user_id 为唯一键的 upsert 流
INSERT INTO iceberg.dim.dim_user (user_id, event_type, url, session_id, device, event_time, latitude, longitude, purchase_amount, updated_at)
SELECT
    user_id, event_type, url, session_id, device,
    event_time, latitude, longitude, purchase_amount,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY rowtime_event_time DESC
        ) AS rn
    FROM clickstream_clean_view
)
WHERE rn = 1;
