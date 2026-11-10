"""catalog 解析与 render_flink_sql 的关键不变量。"""

import pytest

from layering import render_flink_sql
from layering.catalog import REPO_ROOT, load_catalog_config, parse_properties
from layering.contract import (
    DIM_USER,
    DWD_CLICKSTREAM,
    ODS_CLICKSTREAM,
    ODS_CLICKSTREAM_DIRTY,
)

PROPERTIES = REPO_ROOT / "trino" / "iceberg.properties"


def test_parse_properties_skips_comments_and_blanks(tmp_path):
    props_file = tmp_path / "iceberg.properties"
    props_file.write_text(
        "# comment\n"
        "! bang comment\n"
        "a = 1\n"
        "\n"
        "b=two words\n",
        encoding="utf-8",
    )
    props = parse_properties(props_file)
    assert props == {"a": "1", "b": "two words"}


def test_catalog_config_matches_trino_properties():
    config = load_catalog_config(PROPERTIES)
    assert config.catalog_name == "iceberg"
    raw = config.raw_properties
    assert config.flink_options["uri"] == raw["iceberg.rest-catalog.uri"]
    assert config.flink_options["warehouse"] == raw["iceberg.rest-catalog.warehouse"]
    assert config.warehouse == "s3://warehouse/"
    assert config.flink_options["s3.endpoint"] == "http://minio:9000"
    assert config.flink_options["catalog-impl"].endswith("RESTCatalog")


def test_load_catalog_rejects_missing_warehouse(tmp_path):
    props_file = tmp_path / "iceberg.properties"
    props_file.write_text("connector.name=iceberg\niceberg.catalog.type=rest\n", encoding="utf-8")
    with pytest.raises(ValueError, match="warehouse"):
        load_catalog_config(props_file)


def test_render_flink_sql_writes_target_file(tmp_path):
    target = tmp_path / "clickstream_layering.sql"
    sql = render_flink_sql(target)
    assert target.read_text(encoding="utf-8") == sql
    assert target.exists()


def test_rendered_sql_uses_catalog_values_from_properties_not_hardcode(tmp_path):
    sql = render_flink_sql(tmp_path / "job.sql")
    config = load_catalog_config(PROPERTIES)
    assert f"CREATE CATALOG {config.catalog_name}" in sql
    assert f"'warehouse' = '{config.warehouse}'" in sql
    assert f"'uri' = '{config.raw_properties['iceberg.rest-catalog.uri']}'" in sql


def test_rendered_sql_defines_watermark_after_dirty_isolation(tmp_path):
    sql = render_flink_sql(tmp_path / "job.sql")
    # 坏时间判定列与 reject_reason 必须出现在 WATERMARK 之前
    assert sql.index("reject_reason STRING AS") < sql.index("WATERMARK FOR")
    assert sql.index("event_time_calc TIMESTAMP_LTZ") < sql.index("WATERMARK FOR")
    # 坏时间双闸门：正则卡形状 + UNIX_TIMESTAMP IS NOT NULL 卡日历非法（如 02-30）
    assert "event_time REGEXP" in sql
    assert "UNIX_TIMESTAMP(event_time" in sql
    assert "IS NOT NULL" in sql
    assert "TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(event_time" in sql
    # watermark 建立在隔离后的计算列上，而不是原始字符串列
    assert "WATERMARK FOR event_time_calc" in sql


def test_rendered_sql_declares_all_four_tables_with_layering_identifiers(tmp_path):
    sql = render_flink_sql(tmp_path / "job.sql")
    for spec in (ODS_CLICKSTREAM, ODS_CLICKSTREAM_DIRTY, DWD_CLICKSTREAM, DIM_USER):
        assert f"CREATE TABLE iceberg.{spec.full_name}" in sql
        assert f"INSERT INTO iceberg.{spec.full_name}" in sql


def test_rendered_sql_dirty_rows_carry_reject_reason_and_are_not_dropped(tmp_path):
    sql = render_flink_sql(tmp_path / "job.sql")
    assert "clickstream_dirty_view" in sql
    assert "WHERE reject_reason IS NOT NULL" in sql
    # Kafka value metadata 固定为 BYTES，视图里再 CAST 成 STRING 留存
    assert "raw_value BYTES METADATA FROM 'value' VIRTUAL" in sql
    assert "CAST(raw_value AS STRING) AS raw_payload" in sql
    # ODS 原样层没有 WHERE 过滤：全量 append-only
    ods_insert = sql.split("INSERT INTO iceberg.ods.clickstream")[1]
    assert "WHERE" not in ods_insert.split("INSERT INTO")[0]


def test_rendered_sql_upsert_tables_pinned_to_v2_with_primary_keys(tmp_path):
    sql = render_flink_sql(tmp_path / "job.sql")
    assert "PRIMARY KEY (event_id) NOT ENFORCED" in sql
    assert "PRIMARY KEY (user_id) NOT ENFORCED" in sql
    assert sql.count("'format-version' = '2'") == 2
    assert sql.count("'write.upsert.enabled' = 'true'") == 2


def test_rendered_sql_dedup_partitions_by_event_id_and_dim_by_user(tmp_path):
    sql = render_flink_sql(tmp_path / "job.sql")
    assert "PARTITION BY event_id" in sql
    assert "PARTITION BY user_id" in sql
    # dwd 保留首条，dim 取最新一条
    dwd_block = sql.split("INSERT INTO iceberg.dwd.clickstream_dedup")[1]
    dim_block = sql.split("INSERT INTO iceberg.dim.dim_user")[1]
    assert "ORDER BY rowtime_event_time ASC" in dwd_block
    assert "ORDER BY rowtime_event_time DESC" in dim_block
