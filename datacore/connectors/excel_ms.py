"""Conector mínimo para Excel vía Microsoft Graph/SharePoint."""

from __future__ import annotations

from typing import Any

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)


def _rows_to_df(rows: list[Any], spark: SparkSession, header: list[str] | None) -> DataFrame:
    if not rows:
        return spark.createDataFrame([], StructType([]))
    if header:
        mapped = [dict(zip(header, row)) for row in rows]
        return spark.createDataFrame(mapped)
    return spark.createDataFrame(rows)


def read(cfg: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Lee un archivo Excel exportado a CSV o valores tabulares en memoria."""

    if "rows" in cfg:
        return _rows_to_df(cfg["rows"], spark, cfg.get("header"))
    if "csv_url" in cfg:
        resp = requests.get(cfg["csv_url"], timeout=cfg.get("timeout", 30))
        resp.raise_for_status()
        content = resp.text.splitlines()
        reader = spark.read.option("header", str(cfg.get("header", True)).lower()).csv(
            spark.sparkContext.parallelize(content)
        )
        return reader
    if "path" in cfg:
        LOGGER.info("Leyendo Excel exportado desde %s", cfg["path"])
        return spark.read.format(cfg.get("format", "csv")).options(**cfg.get("options", {})).load(
            cfg["path"]
        )
    raise ValueError("Excel requiere 'rows', 'csv_url' o 'path'")


__all__ = ["read"]
