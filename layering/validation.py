"""记录校验：四类脏数据判定的唯一 Python 实现。

Flink 作业、producer、离线校验脚本都调用 :func:`validate_record`，
判定顺序与 ``render_flink_sql`` 生成的 SQL ``CASE`` 完全一致
（顺序常量来自 :mod:`layering.contract`，不在此另立规则）。
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from .contract import (
    ALLOWED_EVENT_TYPES,
    LATITUDE_MAX,
    LATITUDE_MIN,
    LONGITUDE_MAX,
    LONGITUDE_MIN,
    REJECT_INVALID_EVENT_TIME,
    REJECT_INVALID_EVENT_TYPE,
    REJECT_INVALID_GEO,
    REJECT_MISSING_REQUIRED,
    REQUIRED_FIELDS,
)


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    reject_reason: Optional[str]
    # 规范化后的负载：event_time -> datetime(UTC)，geo 拍平为 latitude/longitude；
    # 脏记录为 None，原始 JSON 由调用方自行留存（对应 dirty 表 raw_payload）。
    normalized: Optional[dict]


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _parse_event_time(value: Any) -> Optional[datetime]:
    """解析 ISO-8601 时间；接受 ``Z`` 结尾与无时区（按 UTC 处理）。"""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        # Python 3.11+ 的 fromisoformat 已支持 'Z' 后缀，这里兼容老写法
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_geo(record: Mapping[str, Any]):
    geo = record.get("geo_location")
    if not isinstance(geo, Mapping):
        return None
    try:
        lat = float(geo["lat"])
        lon = float(geo["lon"])
    except (KeyError, TypeError, ValueError):
        return None
    if not (LATITUDE_MIN <= lat <= LATITUDE_MAX):
        return None
    if not (LONGITUDE_MIN <= lon <= LONGITUDE_MAX):
        return None
    return lat, lon


def validate_record(record: Mapping[str, Any]) -> ValidationResult:
    """按契约判定单条记录，返回是否合格、reject_reason 与规范化负载。

    判定优先级（多重缺陷只记第一个原因，与 SQL CASE 同序）：
    1. MISSING_REQUIRED_FIELD  必填字段缺失或空白
    2. INVALID_EVENT_TIME      event_time 无法解析
    3. INVALID_EVENT_TYPE      event_type 不在白名单
    4. INVALID_GEO_LOCATION    geo_location 缺失/非数值/越界
    """
    if not isinstance(record, Mapping):
        return ValidationResult(False, REJECT_MISSING_REQUIRED, None)

    # 1) 必填字段
    for field in REQUIRED_FIELDS:
        if _is_blank(record.get(field)):
            return ValidationResult(False, REJECT_MISSING_REQUIRED, None)

    # 2) event_time 解析——坏时间必须在 watermark 之前隔离，这里先于一切放行
    event_time = _parse_event_time(record["event_time"])
    if event_time is None:
        return ValidationResult(False, REJECT_INVALID_EVENT_TIME, None)

    # 3) event_type 白名单
    if record["event_type"] not in ALLOWED_EVENT_TYPES:
        return ValidationResult(False, REJECT_INVALID_EVENT_TYPE, None)

    # 4) 地理坐标
    geo = _parse_geo(record)
    if geo is None:
        return ValidationResult(False, REJECT_INVALID_GEO, None)
    latitude, longitude = geo

    purchase_amount = record.get("purchase_amount")
    if purchase_amount is not None:
        try:
            purchase_amount = float(purchase_amount)
        except (TypeError, ValueError):
            purchase_amount = None

    normalized = {
        "event_id": str(record["event_id"]),
        "user_id": str(record["user_id"]),
        "event_type": str(record["event_type"]),
        "url": str(record["url"]),
        "session_id": str(record["session_id"]),
        "device": str(record["device"]),
        "event_time": event_time,
        "latitude": latitude,
        "longitude": longitude,
        "purchase_amount": purchase_amount,
    }
    return ValidationResult(True, None, normalized)
