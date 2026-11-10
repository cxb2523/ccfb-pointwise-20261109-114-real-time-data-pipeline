"""validate_record：四类脏数据判定、优先级、规范化输出。"""

from datetime import datetime, timezone

from layering import validate_record
from layering.contract import (
    REJECT_INVALID_EVENT_TIME,
    REJECT_INVALID_EVENT_TYPE,
    REJECT_INVALID_GEO,
    REJECT_MISSING_REQUIRED,
)


def _base_record(**overrides):
    record = {
        "event_id": "evt-1",
        "user_id": "user-1",
        "event_type": "page_view",
        "url": "/home",
        "session_id": "sess-1",
        "device": "mobile",
        "event_time": "2026-11-10T08:00:00Z",
        "geo_location": {"lat": 30.0, "lon": 120.0},
        "purchase_amount": None,
    }
    record.update(overrides)
    return record


def test_valid_record_normalizes_utc_datetime_and_flat_geo():
    result = validate_record(_base_record())
    assert result.valid is True
    assert result.reject_reason is None
    assert result.normalized["event_time"] == datetime(
        2026, 11, 10, 8, 0, 0, tzinfo=timezone.utc
    )
    assert result.normalized["latitude"] == 30.0
    assert result.normalized["longitude"] == 120.0


def test_missing_required_field_variants():
    assert validate_record(_base_record(event_id=None)).reject_reason == REJECT_MISSING_REQUIRED
    assert validate_record(_base_record(url="   ")).reject_reason == REJECT_MISSING_REQUIRED
    assert validate_record({}).reject_reason == REJECT_MISSING_REQUIRED
    assert validate_record("not-a-dict").reject_reason == REJECT_MISSING_REQUIRED


def test_invalid_event_time_variants():
    # 注：空字符串属于 MISSING_REQUIRED_FIELD（空白即缺失，优先级更高）
    for bad in ("not-a-timestamp", "2026-13-40T99:99:99Z", 1234567890):
        record = _base_record(event_time=bad)
        result = validate_record(record)
        assert result.valid is False
        assert result.reject_reason == REJECT_INVALID_EVENT_TIME, bad


def test_event_time_without_timezone_treated_as_utc():
    result = validate_record(_base_record(event_time="2026-11-10T08:00:00"))
    assert result.valid is True
    assert result.normalized["event_time"].tzinfo == timezone.utc


def test_invalid_event_type_not_in_whitelist():
    result = validate_record(_base_record(event_type="sign_up"))
    assert result.valid is False
    assert result.reject_reason == REJECT_INVALID_EVENT_TYPE


def test_every_whitelisted_event_type_passes():
    for event_type in ("page_view", "add_to_cart", "purchase", "logout"):
        assert validate_record(_base_record(event_type=event_type)).valid is True


def test_invalid_geo_variants():
    bad_geos = (
        None,
        {},
        {"lat": 91.0, "lon": 120.0},
        {"lat": 30.0, "lon": -200.0},
        {"lat": "x", "lon": "y"},
    )
    for geo in bad_geos:
        result = validate_record(_base_record(geo_location=geo))
        assert result.reject_reason == REJECT_INVALID_GEO, geo


def test_geo_boundary_values_are_valid():
    assert validate_record(
        _base_record(geo_location={"lat": -90.0, "lon": -180.0})
    ).valid is True
    assert validate_record(
        _base_record(geo_location={"lat": 90.0, "lon": 180.0})
    ).valid is True


def test_reason_priority_missing_beats_bad_time():
    # 同时缺字段且时间坏掉，只能记优先级最高的 MISSING_REQUIRED_FIELD
    result = validate_record(_base_record(event_id="", event_time="garbage"))
    assert result.reject_reason == REJECT_MISSING_REQUIRED


def test_reason_priority_bad_time_beats_bad_type_and_geo():
    result = validate_record(
        _base_record(event_time="garbage", event_type="sign_up", geo_location=None)
    )
    assert result.reject_reason == REJECT_INVALID_EVENT_TIME


def test_reason_priority_bad_type_beats_bad_geo():
    result = validate_record(_base_record(event_type="sign_up", geo_location=None))
    assert result.reject_reason == REJECT_INVALID_EVENT_TYPE
