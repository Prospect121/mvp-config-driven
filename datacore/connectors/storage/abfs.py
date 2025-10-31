"""Conector ABFS."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read(
    spark: SparkSession,
    uri: str,
    fmt: str,
    options: dict[str, Any],
    schema: StructType | None = None,
) -> DataFrame:
    reader = spark.read.format(fmt)
    for key, value in options.items():
        reader = reader.option(key, value)
    if schema is not None:
        reader = reader.schema(schema)
    return reader.load(uri)


def write(
    df: DataFrame,
    uri: str,
    fmt: str,
    mode: str,
    options: dict[str, Any],
    partition_by: list[str] | None = None,
    merge_schema: bool | None = None,
) -> None:
    writer = df.write.mode(mode).format(fmt)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if merge_schema is not None:
        writer = writer.option("mergeSchema", str(merge_schema).lower())
    for key, value in options.items():
        writer = writer.option(key, value)
    writer.save(uri)
