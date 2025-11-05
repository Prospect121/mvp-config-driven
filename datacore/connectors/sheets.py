"""Conector mínimo para Google Sheets."""

from __future__ import annotations

from typing import Any

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)


def _values_to_dataframe(values: list[Any], spark: SparkSession, header: list[str] | None) -> DataFrame:
    if not values:
        return spark.createDataFrame([], StructType([]))
    if header:
        rows = [dict(zip(header, row)) for row in values]
        return spark.createDataFrame(rows)
    return spark.createDataFrame(values)


def read(cfg: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Lee una hoja como DataFrame mediante export CSV o payload embebido."""

    if "values" in cfg:
        header = cfg.get("header")
        return _values_to_dataframe(cfg["values"], spark, header)
    if "csv_url" in cfg:
        resp = requests.get(cfg["csv_url"], timeout=cfg.get("timeout", 30))
        resp.raise_for_status()
        content = resp.text.splitlines()
        rdd = spark.sparkContext.parallelize(content)
        reader = spark.read.option("header", str(cfg.get("header", True)).lower()).csv(rdd)
        return reader
    raise ValueError("Sheets requiere 'values' o 'csv_url'")


__all__ = ["read"]
