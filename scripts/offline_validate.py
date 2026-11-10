"""离线校验：对 JSONL 样例跑同一份 layering.validate_record。

这正是收拢契约的意义——离线脚本不再自己抄白名单和脏数据规则，
与 Flink 作业、producer 共用 layering 包的唯一实现。

用法：
    python scripts/offline_validate.py [clickstream_samples.jsonl]
退出码非 0 表示出现了样例自带判定与契约判定不一致的行（口径漂移）。
"""

import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from layering import validate_record  # noqa: E402


def main() -> int:
    sink = Path(sys.argv[1]) if len(sys.argv) > 1 else REPO_ROOT / "clickstream_samples.jsonl"
    if not sink.exists():
        print(f"样例文件不存在：{sink}（先运行 python producer/producer.py 生成）")
        return 2

    drift = 0
    total = 0
    distribution: Counter = Counter()
    with open(sink, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            if not line.strip():
                continue
            total += 1
            envelope = json.loads(line)
            record = envelope["record"]
            result = validate_record(record)
            distribution[result.reject_reason or "VALID"] += 1

            # producer 落盘时附带的判定必须与当前契约实时判定一致
            if envelope.get("reject_reason") != result.reject_reason:
                drift += 1
                print(
                    f"[口径漂移] 第 {line_no} 行：样例标记 {envelope.get('reject_reason')!r}，"
                    f"契约判定 {result.reject_reason!r}"
                )

    print(f"校验行数: {total}")
    print("reject_reason 分布:")
    for reason in sorted(distribution):
        print(f"  {reason:24s} {distribution[reason]}")

    if drift:
        print(f"发现 {drift} 行口径漂移，离线校验失败")
        return 1
    print("离线校验通过：样例判定与 layering 契约完全一致")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
