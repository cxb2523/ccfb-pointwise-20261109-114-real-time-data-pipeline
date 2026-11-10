"""解析 ``trino/iceberg.properties``，产出 Flink / Trino 共用的 catalog 配置。

Flink 作业和 Trino 连的是同一个 REST catalog 与同一个 warehouse，
所以 Flink catalog DDL 的参数必须由这份 properties 推导，不能另写死一份，
否则 Trino 看不到 Flink 建的表、v2 UPSERT 也可能因表属性不一致退化成追加。
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Optional

# 仓库根目录：layering/catalog.py 向上两级
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROPERTIES_PATH = REPO_ROOT / "trino" / "iceberg.properties"

# Trino properties key -> Flink iceberg catalog option
# 只登记实际需要的键，未知键保留在 raw_properties 里备查
_FLINK_OPTION_MAP = {
    "iceberg.rest-catalog.uri": "uri",
    "iceberg.rest-catalog.warehouse": "warehouse",
    "s3.endpoint": "s3.endpoint",
    "s3.region": "client.region",
    "s3.path-style-access": "s3.path-style-access",
    "s3.aws-access-key": "s3.access-key-id",
    "s3.aws-secret-key": "s3.secret-access-key",
}

_REST_CATALOG_IMPL = {
    "rest": "org.apache.iceberg.rest.RESTCatalog",
}


def parse_properties(path: Path = DEFAULT_PROPERTIES_PATH) -> Dict[str, str]:
    """解析 java .properties 子集：key=value，支持 #/! 注释与行内空白。"""
    props: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith(("#", "!")):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            props[key.strip()] = value.strip()
    return props


@dataclass(frozen=True)
class CatalogConfig:
    """catalog 连接配置，Trino 侧的 catalog 名取自 properties 文件名。"""

    catalog_name: str
    catalog_type: str
    warehouse: str
    file_format: str
    flink_options: Dict[str, str]
    raw_properties: Dict[str, str]

    def require(self, key: str) -> str:
        value = self.raw_properties.get(key)
        if not value:
            raise ValueError(
                f"trino/iceberg.properties 缺少必需项 {key!r}，无法渲染 Flink catalog DDL"
            )
        return value


def load_catalog_config(path: Optional[Path] = None) -> CatalogConfig:
    """读取并转换 Trino iceberg properties。

    catalog 名遵循 Trino 约定：``<catalog-name>.properties`` 的文件名 stem。
    """
    properties_path = Path(path) if path else DEFAULT_PROPERTIES_PATH
    props = parse_properties(properties_path)
    catalog_name = properties_path.stem

    catalog_type = props.get("iceberg.catalog.type", "")
    if catalog_type not in _REST_CATALOG_IMPL:
        raise ValueError(
            f"暂不支持的 iceberg.catalog.type={catalog_type!r}，"
            f"当前支持：{sorted(_REST_CATALOG_IMPL)}"
        )

    flink_options: Dict[str, str] = {
        "type": "iceberg",
        "catalog-impl": _REST_CATALOG_IMPL[catalog_type],
    }
    for trino_key, flink_key in _FLINK_OPTION_MAP.items():
        if trino_key in props:
            flink_options[flink_key] = props[trino_key]
    # 走 S3（MinIO）时显式指定 S3FileIO，与 warehouse 的 s3:// scheme 对齐
    if "s3.endpoint" in props:
        flink_options["io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"

    warehouse = props.get("iceberg.rest-catalog.warehouse", "")
    if not warehouse:
        raise ValueError("trino/iceberg.properties 缺少 iceberg.rest-catalog.warehouse")

    return CatalogConfig(
        catalog_name=catalog_name,
        catalog_type=catalog_type,
        warehouse=warehouse,
        file_format=props.get("iceberg.file-format", "PARQUET").lower(),
        flink_options=flink_options,
        raw_properties=dict(props),
    )


def render_catalog_options(options: Mapping[str, str]) -> str:
    """把 Flink catalog option 映射渲染成 WITH 子句的缩进文本。"""
    return ",\n".join(
        f"    '{key}' = '{value}'" for key, value in options.items()
    )
