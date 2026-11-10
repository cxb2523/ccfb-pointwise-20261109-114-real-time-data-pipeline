"""render_flink_sql：由契约渲染 Flink SQL 作业。

生成的作业拓扑（规则全部来自 :mod:`layering.contract`，无硬编码）：

Kafka source（原始字符串入场，事件时间先判定再转换）
  ├── 全量原样  -> iceberg.ods.clickstream            （append-only）
  ├── 脏记录    -> iceberg.ods.clickstream_dirty      （带 reject_reason，不静默丢）
  └── 合格流    -> ROW_NUMBER(event_id) 去重 -> iceberg.dwd.clickstream_dedup（v2 UPSERT）
               -> ROW_NUMBER(user_id 最新) -> iceberg.dim.dim_user          （v2 UPSERT）

关键约束：event_time 无法解析的记录在 **watermark 定义之前** 就被隔离——
source 里坏时间换算结果为 NULL，不参与 watermark 推进，乱序脏数据既拖不住
watermark，也不会污染 event_id 去重 ROW_NUMBER 的状态排序。
"""

from pathlib import Path
from typing import List, Sequence

from .catalog import (
    REPO_ROOT,
    CatalogConfig,
    load_catalog_config,
    render_catalog_options,
)
from .contract import (
    DIM_USER,
    DWD_CLICKSTREAM,
    EVENT_TYPE_WHITELIST,
    FLINK_TS_FORMAT,
    FLINK_TS_PATTERN,
    LATITUDE_MAX,
    LATITUDE_MIN,
    LONGITUDE_MAX,
    LONGITUDE_MIN,
    ODS_CLICKSTREAM,
    ODS_CLICKSTREAM_DIRTY,
    REJECT_INVALID_EVENT_TIME,
    REJECT_INVALID_EVENT_TYPE,
    REJECT_INVALID_GEO,
    REJECT_MISSING_REQUIRED,
    REQUIRED_FIELDS,
    TableSpec,
    WATERMARK_DELAY_SECONDS,
)

DEFAULT_SQL_PATH = REPO_ROOT / "flink" / "sql-jobs" / "clickstream_layering.sql"

# 与原作业一致的运行参数，额外钉死 UTC，保证 UNIX_TIMESTAMP/CAST 的时区口径
HEADER_SETTINGS = (
    "SET 'execution.runtime-mode' = 'streaming';",
    "SET 'sql-client.execution.attached-execution' = 'false';",
    "SET 'state.backend' = 'rocksdb';",
    "SET 'state.backend.incremental' = 'true';",
    "SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';",
    "SET 'execution.checkpointing.interval' = '10s';",
    "SET 'execution.checkpointing.min-pause' = '10s';",
    "SET 'sql-client.execution.result-mode' = 'TABLEAU';",
    "SET 'parallelism.default' = '1';",
    "SET 'table.local-time-zone' = 'UTC';",
)

JARS = (
    "/opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
    "/opt/flink/lib/flink-json-1.18.1.jar",
    "/opt/flink/lib/iceberg-flink-runtime-1.18-1.5.0.jar",
    "/opt/flink/lib/hadoop-common-2.8.3.jar",
    "/opt/flink/lib/hadoop-hdfs-2.8.3.jar",
    "/opt/flink/lib/hadoop-client-2.8.3.jar",
    "/opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar",
    "/opt/flink/lib/bundle-2.20.18.jar",
)

# Kafka source 的原始字段（列名与消息 JSON key 一一对应，event_time 以 STRING 原样入场）
SOURCE_RAW_COLUMNS = (
    "event_id",
    "user_id",
    "event_type",
    "url",
    "session_id",
    "device",
    "event_time",
    "geo_location",
    "purchase_amount",
)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _missing_required_expr(fields: Sequence[str] = REQUIRED_FIELDS) -> str:
    clauses = [f"COALESCE(TRIM({field}), '') = ''" for field in fields]
    return "(" + " OR ".join(clauses) + ")"


def _event_time_valid_expr() -> str:
    # 双重闸门：正则卡形状，UNIX_TIMESTAMP IS NOT NULL 卡日历合法性
    # （如 02-30 正则能过但转换为 NULL），与 Python validate_record 的 fromisoformat 同口径
    return (
        f"(event_time IS NOT NULL AND event_time REGEXP {_sql_literal(FLINK_TS_PATTERN)} "
        f"AND UNIX_TIMESTAMP(event_time, {_sql_literal(FLINK_TS_FORMAT)}) IS NOT NULL)"
    )


def _geo_valid_expr() -> str:
    return (
        "(geo_location IS NOT NULL "
        "AND geo_location.lat IS NOT NULL AND geo_location.lon IS NOT NULL "
        f"AND geo_location.lat BETWEEN {LATITUDE_MIN} AND {LATITUDE_MAX} "
        f"AND geo_location.lon BETWEEN {LONGITUDE_MIN} AND {LONGITUDE_MAX})"
    )


def _render_catalog(catalog: CatalogConfig) -> List[str]:
    lines = [
        f"DROP CATALOG IF EXISTS {catalog.catalog_name};",
        f"CREATE CATALOG {catalog.catalog_name} WITH (",
        render_catalog_options(catalog.flink_options),
        ");",
        "CREATE DATABASE IF NOT EXISTS "
        f"{catalog.catalog_name}.{ODS_CLICKSTREAM.database};",
        "CREATE DATABASE IF NOT EXISTS "
        f"{catalog.catalog_name}.{DWD_CLICKSTREAM.database};",
        "CREATE DATABASE IF NOT EXISTS "
        f"{catalog.catalog_name}.{DIM_USER.database};",
    ]
    return lines


def _render_source_ddl() -> List[str]:
    # 所有判定列只基于原始列，坏值不进转换函数；计算列之间不互相引用
    missing_expr = _missing_required_expr()
    ts_valid_expr = _event_time_valid_expr()
    type_list = ", ".join(_sql_literal(v) for v in EVENT_TYPE_WHITELIST)
    geo_expr = _geo_valid_expr()
    reject_case = f"""        CASE
            WHEN {missing_expr} THEN {_sql_literal(REJECT_MISSING_REQUIRED)}
            WHEN NOT {ts_valid_expr} THEN {_sql_literal(REJECT_INVALID_EVENT_TIME)}
            WHEN event_type IS NULL OR event_type NOT IN ({type_list})
                THEN {_sql_literal(REJECT_INVALID_EVENT_TYPE)}
            WHEN NOT {geo_expr} THEN {_sql_literal(REJECT_INVALID_GEO)}
            ELSE CAST(NULL AS STRING)
        END"""

    lines = [
        "DROP TABLE IF EXISTS clickstream_source;",
        """CREATE TABLE clickstream_source (
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
    required_missing BOOLEAN AS {missing},
    event_time_valid BOOLEAN AS {ts_valid},
    event_type_valid BOOLEAN AS (event_type IS NOT NULL AND event_type IN ({type_list})),
    geo_valid BOOLEAN AS {geo},
    -- 坏时间换算成 NULL 而不是抛异常：TO_TIMESTAMP_LTZ 只在正则放行后才调用
    event_time_calc TIMESTAMP_LTZ(3) AS (
        CASE WHEN {ts_valid}
             THEN TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(event_time, {ts_fmt}), 0)
             ELSE CAST(NULL AS TIMESTAMP_LTZ(3))
        END
    ),
    reject_reason STRING AS (
{reject_case}
    ),
    -- watermark 必须定义在所有判定/隔离列之后：坏记录 event_time_calc 为 NULL，
    -- 不参与 watermark 推进，乱序脏数据拖不住 watermark，去重排序也不被污染
    WATERMARK FOR event_time_calc AS event_time_calc
        - INTERVAL '{delay}' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clickstream',
    'properties.bootstrap.servers' = 'broker:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'ISO-8601'
);""".format(
            missing=missing_expr,
            ts_valid=ts_valid_expr,
            type_list=type_list,
            geo=geo_expr,
            ts_fmt=_sql_literal(FLINK_TS_FORMAT),
            reject_case=reject_case,
            delay=WATERMARK_DELAY_SECONDS,
        ),
    ]
    return lines


def _render_table_ddl(catalog_name: str, spec: TableSpec) -> str:
    column_lines = [
        f"    {col.name} {col.flink_type}" + (f" COMMENT {_sql_literal(col.comment)}" if col.comment else "")
        for col in spec.columns
    ]
    if spec.primary_key:
        column_lines.append(
            "    PRIMARY KEY (" + ", ".join(spec.primary_key) + ") NOT ENFORCED"
        )
    # 显式钉住 Iceberg v2 + upsert，防止 UPSERT 退化成追加
    options = ["    'format' = 'parquet'"]
    if spec.upsert:
        options.append("    'format-version' = '2'")
        options.append("    'write.upsert.enabled' = 'true'")
    return (
        f"DROP TABLE IF EXISTS {catalog_name}.{spec.full_name};\n"
        f"CREATE TABLE {catalog_name}.{spec.full_name} (\n"
        + ",\n".join(column_lines)
        + "\n) WITH (\n"
        + ",\n".join(options)
        + "\n);"
    )


def _render_views() -> List[str]:
    dirty_view = """CREATE TEMPORARY VIEW clickstream_dirty_view AS
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
WHERE reject_reason IS NOT NULL;"""

    clean_view = """CREATE TEMPORARY VIEW clickstream_clean_view AS
SELECT
    event_id, user_id, event_type, url, session_id, device,
    CAST(event_time_calc AS TIMESTAMP(6)) AS event_time,
    event_time_calc AS rowtime_event_time,
    geo_location.lat AS latitude,
    geo_location.lon AS longitude,
    purchase_amount
FROM clickstream_source
WHERE reject_reason IS NULL;"""
    return [dirty_view, clean_view]


def _render_inserts(catalog_name: str) -> List[str]:
    ods_cols = ", ".join(c.name for c in ODS_CLICKSTREAM.columns)
    ods_insert = f"""-- ODS 原样层：source 全量 append-only 写入（坏 event_time 落 NULL，原始 JSON 在 dirty 表留存）
INSERT INTO {catalog_name}.{ODS_CLICKSTREAM.full_name} ({ods_cols})
SELECT
    event_id, user_id, event_type, url, session_id, device,
    CAST(event_time_calc AS TIMESTAMP(6)),
    geo_location.lat, geo_location.lon, purchase_amount,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))
FROM clickstream_source;"""

    dirty_cols = ", ".join(c.name for c in ODS_CLICKSTREAM_DIRTY.columns)
    dirty_insert = f"""-- 脏数据隔离层：带 reject_reason，禁止静默丢弃
INSERT INTO {catalog_name}.{ODS_CLICKSTREAM_DIRTY.full_name} ({dirty_cols})
SELECT
    event_id, user_id, event_type, url, session_id, device,
    event_time, latitude, longitude, purchase_amount,
    reject_reason, raw_payload, rejected_at
FROM clickstream_dirty_view;"""

    dwd_cols = ", ".join(c.name for c in DWD_CLICKSTREAM.columns)
    dwd_insert = f"""-- DWD 明细层：合格流按 event_id 去重（首条生效），Iceberg v2 UPSERT
-- 规范 DEDUP 模式：ROW_NUMBER + 单一 rowtime 排序键，输出以 event_id 为唯一键的 upsert 流
INSERT INTO {catalog_name}.{DWD_CLICKSTREAM.full_name} ({dwd_cols})
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
WHERE rn = 1;"""

    dim_cols = ", ".join(c.name for c in DIM_USER.columns)
    dim_insert = f"""-- DIM 用户层：按 user_id 取 event_time 最新的 profile，Iceberg v2 UPSERT
-- last-row DEDUP：rowtime 倒序 rn=1，输出以 user_id 为唯一键的 upsert 流
INSERT INTO {catalog_name}.{DIM_USER.full_name} ({dim_cols})
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
WHERE rn = 1;"""

    return [ods_insert, dirty_insert, dwd_insert, dim_insert]


def render_flink_sql(output_path: Path = DEFAULT_SQL_PATH) -> str:
    """渲染完整 Flink SQL 作业并写入 ``output_path``（默认 clickstream_layering.sql）。

    返回 SQL 文本；catalog/warehouse 等连接参数从 trino/iceberg.properties 解析。
    """
    catalog = load_catalog_config()
    sections: List[str] = []

    sections.append("-- Flink 运行参数\n" + "\n".join(HEADER_SETTINGS))
    sections.append(
        "-- 加载 connector JAR\n"
        + "\n".join(f"ADD JAR {_sql_literal(jar)};" for jar in JARS)
        + "\n\nSHOW JARS;"
    )
    sections.append("-- Catalog/数据库：参数解析自 trino/iceberg.properties\n"
                    + "\n\n".join(_render_catalog(catalog)))
    sections.append(
        "-- Kafka source：原始字符串入场，event_time 先判定后转换，watermark 在隔离列之后\n"
        + "\n\n".join(_render_source_ddl())
    )

    table_ddls = [
        _render_table_ddl(catalog.catalog_name, ODS_CLICKSTREAM),
        _render_table_ddl(catalog.catalog_name, ODS_CLICKSTREAM_DIRTY),
        _render_table_ddl(catalog.catalog_name, DWD_CLICKSTREAM),
        _render_table_ddl(catalog.catalog_name, DIM_USER),
    ]
    sections.append("-- 四张 Iceberg 表（schema 来自 layering.contract）\n"
                    + "\n\n".join(table_ddls))
    sections.append("-- 分流视图\n" + "\n\n".join(_render_views()))
    sections.append("-- 四个 sink\n" + "\n\n".join(_render_inserts(catalog.catalog_name)))

    sql = "\n\n".join(sections) + "\n"

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(sql, encoding="utf-8")
    return sql
