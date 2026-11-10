"""layering — 统一的点击流分层契约包。

Flink 作业、离线校验、Trino 核对共用同一份规则实现，
对外只暴露 :func:`validate_record` 与 :func:`render_flink_sql` 两个接口
（见 ``layering/__init__.py`` 的 ``__all__``）。
"""

from .validation import validate_record
from .sql import render_flink_sql

__all__ = ["validate_record", "render_flink_sql"]
