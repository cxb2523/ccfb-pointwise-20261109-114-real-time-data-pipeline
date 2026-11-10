"""双 sink 点击流样例生成器。

规则不在本文件内抄写：event_type 白名单、时间格式、脏数据判定全部 import 自
``layering`` 包，Flink 作业与离线 JSONL 核对因此共用同一套契约。

sink 行为：
- 设置了 ``KAFKA_BROKER`` 且环境里装了 ``confluent_kafka`` -> Kafka + JSONL 双写；
- 否则（本地没装 MQ 客户端）-> 只写 JSONL，照样能离线核对四类脏数据。

固定产出 :data:`TOTAL_RECORDS` 条样例，其中确定性地混入：
- 四类脏数据各 2 条（共 8 条）；
- 2 组重复 event_id（验证 dwd 去重）；
- 同一 user_id 的多版本事件（验证 dim_user 取最新 profile）。
"""

import json
import logging
import os
import socket
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

# producer.py 位于 <repo>/producer/ 下，仓库根加入 sys.path 后才能 import layering
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from layering import validate_record  # noqa: E402
from layering.contract import (  # noqa: E402
    DEVICE_WHITELIST,
    EVENT_TIME_FORMAT,
    EVENT_TYPE_WHITELIST,
    REJECT_INVALID_EVENT_TIME,
    REJECT_INVALID_EVENT_TYPE,
    REJECT_INVALID_GEO,
    REJECT_MISSING_REQUIRED,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("clickstream-producer")

TOTAL_RECORDS = 50
TOPIC = "clickstream"
JSONL_SINK = Path(
    os.getenv("JSONL_SINK", str(REPO_ROOT / "clickstream_samples.jsonl"))
)

fake = Faker()
Faker.seed(20261110)

_BASE_TIME = datetime(2026, 11, 10, 8, 0, 0, tzinfo=timezone.utc)
_USER_IDS = [f"user-{i:02d}" for i in range(1, 7)]


def _ts(minutes_offset: int) -> str:
    return (_BASE_TIME + timedelta(minutes=minutes_offset)).strftime(EVENT_TIME_FORMAT)


def _clean_record(seq: int, **overrides) -> dict:
    """构造一条默认合格的记录，overrides 可定向制造脏数据。

    注意 event_id/user_id 不设具名默认参数：否则 ``event_id=None`` 会在
    默认字典里被 ``or`` 吞掉，覆盖不生效，脏数据就造不出来。
    """
    record = {
        "event_id": f"evt-{seq:03d}",
        "user_id": fake.random_element(_USER_IDS),
        "event_type": fake.random_element(EVENT_TYPE_WHITELIST),
        "url": fake.uri_path(),
        "session_id": f"sess-{seq:03d}",
        "device": fake.random_element(DEVICE_WHITELIST),
        "event_time": _ts(seq),
        "geo_location": {
            "lat": float(fake.latitude()),
            "lon": float(fake.longitude()),
        },
        "purchase_amount": (
            round(fake.pyfloat(min_value=0.0, max_value=500.0), 2)
            if fake.boolean(chance_of_getting_true=30)
            else None
        ),
    }
    record.update(overrides)
    return record


def build_sample_batch() -> list:
    """确定性地拼出 50 条样例：42 条合格（含 2 组重复 event_id）+ 8 条四类脏数据。"""
    records = [_clean_record(seq) for seq in range(42)]

    # 重复 event_id：同 ID 两条、后到的时间更晚，dwd 去重后每个 ID 只留一行
    records[12]["event_id"] = "evt-dup-A"
    records[25] = _clean_record(
        25, event_id="evt-dup-A", user_id=records[12]["user_id"]
    )
    records[8]["event_id"] = "evt-dup-B"
    records[30] = _clean_record(
        30, event_id="evt-dup-B", user_id=records[8]["user_id"]
    )

    # 专用 user_id（不在随机池 _USER_IDS 内）：它的事件只有下面这三条，
    # event_time 递增，dim_user 取最新 profile 的核对结果因此唯一确定 -> /checkout
    profile_user = "user-latest-01"
    for seq, url in ((2, "/home"), (20, "/cart"), (38, "/checkout")):
        records[seq] = _clean_record(
            seq,
            user_id=profile_user,
            event_type="purchase",
            url=url,
            device="mobile",
        )

    # --- 四类脏数据，每类 2 条，reject_reason 由 layering.validate_record 统一判定 ---
    dirty = [
        # 1) MISSING_REQUIRED_FIELD：必填字段为 None / 空白
        _clean_record(42, event_id=None),
        _clean_record(43, url="   "),
        # 2) INVALID_EVENT_TIME：一个形状就不对，一个形状合法但日历非法（02-30），
        #    后者专门验证 SQL 的 UNIX_TIMESTAMP IS NOT NULL 第二道闸门，
        #    必须在 watermark 前被隔离
        _clean_record(44, event_time="not-a-timestamp"),
        _clean_record(45, event_time="2026-02-30T08:00:00Z"),
        # 3) INVALID_EVENT_TYPE：白名单之外的事件
        _clean_record(46, event_type="sign_up"),
        _clean_record(47, event_type="refund"),
        # 4) INVALID_GEO_LOCATION：坐标越界 / 结构缺失
        _clean_record(48, geo_location={"lat": 200.0, "lon": 120.0}),
        _clean_record(49, geo_location=None),
    ]
    records.extend(dirty)
    assert len(records) == TOTAL_RECORDS
    return records


def write_jsonl_sink(records: list, results: list) -> None:
    """无 MQ 依赖也能跑的 sink：逐条落 JSONL，附带契约判定结果供离线核对。"""
    JSONL_SINK.parent.mkdir(parents=True, exist_ok=True)
    with open(JSONL_SINK, "w", encoding="utf-8") as fh:
        for record, result in zip(records, results):
            fh.write(
                json.dumps(
                    {
                        "record": record,
                        "valid": result.valid,
                        "reject_reason": result.reject_reason,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    logger.info("JSONL sink 写入 %d 条 -> %s", len(records), JSONL_SINK)


def build_kafka_producer(broker: str):
    # 延迟 import：只有真正走 Kafka 发送分支时才需要 confluent_kafka，
    # 没装 MQ 依赖的环境不受影响，JSONL sink 照常工作。
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": broker})


def _wait_for_broker(broker: str, timeout_seconds: int = 60) -> bool:
    """有限等待 broker TCP 可达：生成器只跑 50 条就退出，避免启动早于 Kafka 时丢消息。"""
    host, _, port_text = broker.partition(":")
    port = int(port_text or "9092")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=3):
                return True
        except (OSError, ValueError):
            logger.info("等待 Kafka broker %s:%s 就绪...", host, port)
            time.sleep(3)
    return False


def _delivery_report(err, msg):
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.info(
            "Message delivered to %s [%s]", msg.topic(), msg.partition()
        )


def send_to_kafka(producer, records: list) -> None:
    for record in records:
        producer.produce(
            TOPIC,
            key=record.get("session_id"),
            value=json.dumps(record, ensure_ascii=False),
            callback=_delivery_report,
        )
        producer.poll(0)
    producer.flush()
    logger.info("Kafka sink 发送 %d 条 -> topic %s", len(records), TOPIC)


def main() -> int:
    records = build_sample_batch()

    # 同一份 validate_record 实现：producer、Flink 作业、离线核对口径一致
    results = [validate_record(record) for record in records]
    reason_counts = Counter(
        result.reject_reason or "VALID" for result in results
    )
    logger.info("样例判定分布: %s", dict(sorted(reason_counts.items())))

    expected_dirty = {
        REJECT_MISSING_REQUIRED: 2,
        REJECT_INVALID_EVENT_TIME: 2,
        REJECT_INVALID_EVENT_TYPE: 2,
        REJECT_INVALID_GEO: 2,
    }
    for reason, count in expected_dirty.items():
        actual = reason_counts.get(reason, 0)
        if actual != count:
            raise RuntimeError(
                f"样例契约自检失败：{reason} 期望 {count} 条，实际 {actual} 条"
            )
    logger.info("四类脏数据自检通过，每类各 2 条")

    write_jsonl_sink(records, results)

    broker = os.getenv("KAFKA_BROKER")
    if not broker:
        logger.warning("未设置 KAFKA_BROKER，跳过 Kafka 发送，仅保留 JSONL sink")
        return 0
    if not _wait_for_broker(broker):
        logger.error("等待 Kafka broker 超时，跳过发送，JSONL 样例仍保留在 %s", JSONL_SINK)
        return 1
    try:
        producer = build_kafka_producer(broker)
    except ImportError:
        logger.warning(
            "confluent_kafka 未安装，跳过 Kafka 发送，仅保留 JSONL sink"
        )
        return 0
    send_to_kafka(producer, records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
