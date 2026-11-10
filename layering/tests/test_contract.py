"""契约层不变量：白名单、四类 reject_reason、四张表的 schema/主键/UPSERT 属性。"""

from layering.contract import (
    ALLOWED_EVENT_TYPES,
    DIM_USER,
    DWD_CLICKSTREAM,
    EVENT_TYPE_WHITELIST,
    ODS_CLICKSTREAM,
    ODS_CLICKSTREAM_DIRTY,
    REJECT_REASONS,
    TABLES,
    TABLES_BY_NAME,
)


def test_whitelist_is_single_source_of_truth():
    assert EVENT_TYPE_WHITELIST == ("page_view", "add_to_cart", "purchase", "logout")
    assert ALLOWED_EVENT_TYPES == frozenset(EVENT_TYPE_WHITELIST)


def test_exactly_four_reject_reasons():
    assert len(REJECT_REASONS) == 4
    assert len(set(REJECT_REASONS)) == 4
    # 优先级：缺字段 > 坏时间 > 白名单外事件 > 坏坐标
    assert REJECT_REASONS == (
        "MISSING_REQUIRED_FIELD",
        "INVALID_EVENT_TIME",
        "INVALID_EVENT_TYPE",
        "INVALID_GEO_LOCATION",
    )


def test_four_tables_registered_with_layering_names():
    assert {spec.full_name for spec in TABLES} == {
        "ods.clickstream",
        "ods.clickstream_dirty",
        "dwd.clickstream_dedup",
        "dim.dim_user",
    }
    assert set(TABLES_BY_NAME) == {spec.full_name for spec in TABLES}


def test_ods_tables_are_append_only():
    assert ODS_CLICKSTREAM.upsert is False
    assert ODS_CLICKSTREAM.primary_key == ()
    assert ODS_CLICKSTREAM_DIRTY.upsert is False


def test_dirty_table_carries_reject_reason_and_raw_payload():
    names = {c.name for c in ODS_CLICKSTREAM_DIRTY.columns}
    assert {"reject_reason", "raw_payload", "rejected_at"} <= names


def test_dwd_and_dim_are_v2_upsert_with_primary_keys():
    assert DWD_CLICKSTREAM.upsert is True
    assert DWD_CLICKSTREAM.primary_key == ("event_id",)
    assert DIM_USER.upsert is True
    assert DIM_USER.primary_key == ("user_id",)


def test_dwd_shares_payload_columns_with_ods():
    ods_names = {c.name for c in ODS_CLICKSTREAM.columns} - {"ingested_at"}
    dwd_names = {c.name for c in DWD_CLICKSTREAM.columns} - {"processed_at"}
    assert ods_names == dwd_names


def test_dim_user_keyed_by_user_and_tracks_latest_profile():
    names = {c.name for c in DIM_USER.columns}
    assert "user_id" in names and "event_time" in names and "updated_at" in names
