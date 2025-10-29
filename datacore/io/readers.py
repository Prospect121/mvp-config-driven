"""Lectores genéricos."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from datacore.connectors.db import jdbc
from datacore.connectors.storage import abfs, gcs, local, s3
from datacore.platforms.base import PlatformBase


def read_batch(spark: SparkSession, platform: PlatformBase, source: dict[str, Any]) -> DataFrame:
    source_type = source["type"]
    fmt = source.get("format", "parquet")
    options = source.get("options", {})
    if source_type == "storage":
        uri = platform.normalize_uri(source["uri"])
        backend = source.get("backend", platform.name)
        if backend == "aws":
            return s3.read(spark, uri, fmt, options)
        if backend == "azure":
            return abfs.read(spark, uri, fmt, options)
        if backend == "gcp":
            return gcs.read(spark, uri, fmt, options)
        return local.read(spark, uri, fmt, options)
    if source_type == "jdbc":
        return jdbc.read(spark, source)
    raise ValueError(f"Tipo de origen no soportado: {source_type}")


def read_stream(spark: SparkSession, platform: PlatformBase, source: dict[str, Any]) -> DataFrame:
    source_type = source["type"]
    fmt = source.get("format", "parquet")
    options = source.get("options", {})
    if source_type == "storage":
        uri = platform.normalize_uri(source["uri"])
        reader = spark.readStream.format(fmt)
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load(uri)
    raise ValueError(f"Streaming no soportado para {source_type}")
