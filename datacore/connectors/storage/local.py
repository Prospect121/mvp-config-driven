"""Conector local/MinIO."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession


def read(spark: SparkSession, uri: str, fmt: str, options: dict[str, Any]) -> DataFrame:
    reader = spark.read.format(fmt)
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load(uri)


def write(df: DataFrame, uri: str, fmt: str, mode: str, options: dict[str, Any]) -> None:
    writer = df.write.mode(mode).format(fmt)
    for key, value in options.items():
        writer = writer.option(key, value)
    writer.save(uri)
