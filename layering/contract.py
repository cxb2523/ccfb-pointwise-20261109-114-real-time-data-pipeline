"""契约层：四张 Iceberg 表的 schema、event_type 白名单、四类脏数据判定常量。

任何模块（Flink SQL 渲染、producer、离线校验、测试）都只能从这里取规则，
禁止在别处再抄一份白名单或 reject_reason 字面量。
"""

from dataclasses import dataclass
from typing import FrozenSet, Tuple


# ---------------------------------------------------------------------------
# 业务规则：event_type 白名单（唯一事实来源）
# ---------------------------------------------------------------------------
EVENT_TYPE_WHITELIST: Tuple[str, ...] = (
    "page_view",
    "add_to_cart",
    "purchase",
    "logout",
)
ALLOWED_EVENT_TYPES: FrozenSet[str] = frozenset(EVENT_TYPE_WHITELIST)

# 设备白名单仅用于生成器取样，不属于四类脏数据判定，不在这里做强校验。
DEVICE_WHITELIST: Tuple[str, ...] = ("mobile", "desktop", "tablet")


# ---------------------------------------------------------------------------
# 四类脏数据 reject_reason，顺序即多重缺陷时的判定优先级（SQL 的 CASE 同序）
# ---------------------------------------------------------------------------
REJECT_MISSING_REQUIRED = "MISSING_REQUIRED_FIELD"
REJECT_INVALID_EVENT_TIME = "INVALID_EVENT_TIME"
REJECT_INVALID_EVENT_TYPE = "INVALID_EVENT_TYPE"
REJECT_INVALID_GEO = "INVALID_GEO_LOCATION"

# 顺序即优先级：缺字段 > 时间解析失败 > 白名单外事件 > 地理坐标非法
REJECT_REASONS: Tuple[str, ...] = (
    REJECT_MISSING_REQUIRED,
    REJECT_INVALID_EVENT_TIME,
    REJECT_INVALID_EVENT_TYPE,
    REJECT_INVALID_GEO,
)

# validate_record 必须逐条给出判定的必填字段（None 或空白字符串都算缺失）
REQUIRED_FIELDS: Tuple[str, ...] = (
    "event_id",
    "user_id",
    "event_type",
    "url",
    "session_id",
    "device",
    "event_time",
)

# 经纬度合法区间
LATITUDE_MIN, LATITUDE_MAX = -90.0, 90.0
LONGITUDE_MIN, LONGITUDE_MAX = -180.0, 180.0

# event_time 的唯一 canonical 格式：producer 按此产出，Flink 按此解析。
# Python 侧 validate_record 用 fromisoformat 兼容更多 ISO 变体，但样例与 SQL
# 都以这个 Z 结尾、秒级精度的格式为准，避免两层口径漂移。
EVENT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# Flink UNIX_TIMESTAMP 的格式串（SQL 字符串字面量里单引号需双写，由 sql.py 转义）
FLINK_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
# 入场前的 NULL 安全正则：形状不符直接判 INVALID_EVENT_TIME，
# 绝不让坏字符串进 TO_TIMESTAMP 把作业打挂。范围卡到月/日/时/分；
# 形状通过但日历非法（如 02-30）的残余情况由 UNIX_TIMESTAMP 返回 NULL 兜底，
# sql.py 的有效判定同时要求正则匹配且转换非 NULL，与 Python fromisoformat 同口径。
FLINK_TS_PATTERN = (
    r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
    r"T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z$"
)
# watermark 允许的乱序容忍度
WATERMARK_DELAY_SECONDS = 5


# ---------------------------------------------------------------------------
# 四张 Iceberg 表的 schema（Flink DDL 与 Trino 类型同时登记，杜绝两边漂移）
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Column:
    name: str
    flink_type: str
    trino_type: str
    comment: str = ""


@dataclass(frozen=True)
class TableSpec:
    database: str
    table: str
    columns: Tuple[Column, ...]
    primary_key: Tuple[str, ...] = ()
    upsert: bool = False

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.table}"


# 公共业务列（ODS 原样层 / DWD 去重层共用）
_PAYLOAD_COLUMNS: Tuple[Column, ...] = (
    Column("event_id", "STRING", "VARCHAR", "事件唯一 ID"),
    Column("user_id", "STRING", "VARCHAR", "用户 ID"),
    Column("event_type", "STRING", "VARCHAR", "事件类型，白名单见契约"),
    Column("url", "STRING", "VARCHAR", "事件发生页面"),
    Column("session_id", "STRING", "VARCHAR", "会话 ID"),
    Column("device", "STRING", "VARCHAR", "设备类型"),
    Column("event_time", "TIMESTAMP(6)", "TIMESTAMP(6)", "事件时间（UTC）"),
    Column("latitude", "DOUBLE", "DOUBLE", "纬度"),
    Column("longitude", "DOUBLE", "DOUBLE", "经度"),
    Column("purchase_amount", "DOUBLE", "DOUBLE", "消费金额，可空"),
)

# 1) ODS 原样层：append-only，Kafka 来什么存什么（字段尽力转换，行不丢）
ODS_CLICKSTREAM = TableSpec(
    database="ods",
    table="clickstream",
    columns=_PAYLOAD_COLUMNS
    + (Column("ingested_at", "TIMESTAMP(6)", "TIMESTAMP(6)", "入湖时间"),),
)

# 2) ODS 脏数据隔离层：append-only，带 reject_reason，禁止静默丢弃
ODS_CLICKSTREAM_DIRTY = TableSpec(
    database="ods",
    table="clickstream_dirty",
    columns=tuple(
        # 脏数据负载列全部可空：坏掉的记录本来就可能缺字段/类型不可转
        Column(c.name, c.flink_type, c.trino_type, c.comment) for c in _PAYLOAD_COLUMNS
    )
    + (
        Column("reject_reason", "STRING", "VARCHAR", "脏数据判定原因（四类之一）"),
        Column("raw_payload", "STRING", "VARCHAR", "原始 Kafka 消息 JSON"),
        Column("rejected_at", "TIMESTAMP(6)", "TIMESTAMP(6)", "隔离时间"),
    ),
)

# 3) DWD 明细层：合格流按 event_id 去重，Iceberg v2 UPSERT
DWD_CLICKSTREAM = TableSpec(
    database="dwd",
    table="clickstream_dedup",
    columns=_PAYLOAD_COLUMNS
    + (Column("processed_at", "TIMESTAMP(6)", "TIMESTAMP(6)", "处理时间"),),
    primary_key=("event_id",),
    upsert=True,
)

# 4) DIM 用户维度层：按 user_id UPSERT 最新 profile
DIM_USER = TableSpec(
    database="dim",
    table="dim_user",
    columns=(
        Column("user_id", "STRING", "VARCHAR", "用户 ID（主键）"),
        Column("event_type", "STRING", "VARCHAR", "最近一次事件类型"),
        Column("url", "STRING", "VARCHAR", "最近一次访问页面"),
        Column("session_id", "STRING", "VARCHAR", "最近一次会话 ID"),
        Column("device", "STRING", "VARCHAR", "最近一次设备类型"),
        Column("event_time", "TIMESTAMP(6)", "TIMESTAMP(6)", "最近一次事件时间"),
        Column("latitude", "DOUBLE", "DOUBLE", "最近一次纬度"),
        Column("longitude", "DOUBLE", "DOUBLE", "最近一次经度"),
        Column("purchase_amount", "DOUBLE", "DOUBLE", "最近一次消费金额"),
        Column("updated_at", "TIMESTAMP(6)", "TIMESTAMP(6)", "profile 更新时间"),
    ),
    primary_key=("user_id",),
    upsert=True,
)

# 注册表：render_flink_sql 与离线核对都遍历它
TABLES: Tuple[TableSpec, ...] = (
    ODS_CLICKSTREAM,
    ODS_CLICKSTREAM_DIRTY,
    DWD_CLICKSTREAM,
    DIM_USER,
)
TABLES_BY_NAME = {spec.full_name: spec for spec in TABLES}
