"""Conector simplificado para fuentes SFTP/FTP."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)


def _reader(spark: SparkSession, cfg: dict[str, Any]):
    fmt = cfg.get("format", "csv")
    reader = spark.read.format(fmt)
    options = cfg.get("options", {})
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader


def read(cfg: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Lee archivos remotos usando un esquema accesible por Spark."""

    path = cfg.get("path") or cfg.get("uri")
    if not path:
        raise ValueError("SFTP requiere 'path' o 'uri'")
    LOGGER.info("Leyendo datos SFTP desde %s", path)
    return _reader(spark, cfg).load(path)


__all__ = ["read"]
