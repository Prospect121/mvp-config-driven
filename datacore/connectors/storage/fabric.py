"""Fabric Lakehouse storage connector."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from datacore.providers.fabric.uris import (
    FabricLakehouseReference,
    parse_fabric_lakehouse_uri,
    resolve_fabric_files_path,
)
from datacore.utils.logging import get_logger

from . import local


LOGGER = get_logger(__name__)


def _apply_common_writer(
    df: DataFrame,
    fmt: str,
    options: dict[str, Any],
    mode: str | None,
    partition_by: list[str] | None,
    merge_schema: bool | None,
):
    writer = df.write.format(fmt)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if merge_schema is not None:
        writer = writer.option("mergeSchema", str(merge_schema).lower())
    for key, value in options.items():
        writer = writer.option(key, value)
    resolved_mode = mode or "append"
    return writer.mode(resolved_mode)


def _log_managed_table(event: str, ref: FabricLakehouseReference, *, mode: str) -> None:
    LOGGER.info(
        "Fabric managed table operation",
        extra={
            "event": event,
            "backend": "fabric",
            "lakehouse": ref.lakehouse,
            "schema": ref.schema,
            "table": ref.table,
            "mode": mode,
        },
    )


def read(
    spark: SparkSession,
    uri: str,
    fmt: str,
    options: dict[str, Any],
    *,
    schema: StructType | None = None,
):
    ref = parse_fabric_lakehouse_uri(uri)
    if ref.is_lakehouse and ref.kind == "tables" and ref.full_table_name:
        _log_managed_table("fabric_read_managed_table", ref, mode="read")
        return spark.table(ref.full_table_name)
    if ref.is_lakehouse and ref.kind == "files" and ref.lakehouse:
        resolved = resolve_fabric_files_path(ref)
        reader = spark.read.format(fmt)
        for key, value in options.items():
            reader = reader.option(key, value)
        if schema is not None:
            reader = reader.schema(schema)
        LOGGER.info(
            "Fabric files read",
            extra={
                "event": "fabric_read_files_path",
                "backend": "fabric",
                "lakehouse": ref.lakehouse,
                "resolved_path": resolved,
            },
        )
        return reader.load(resolved)
    return local.read(spark, uri, fmt, options, schema=schema)


def write(
    df: DataFrame,
    uri: str,
    fmt: str,
    mode: str,
    options: dict[str, Any],
    *,
    partition_by: list[str] | None = None,
    merge_schema: bool | None = None,
) -> None:
    ref = parse_fabric_lakehouse_uri(uri)
    normalized_mode = (mode or "").lower()

    if ref.is_lakehouse and ref.kind == "tables" and ref.full_table_name:
        writer = df.write.format(fmt)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if merge_schema is not None:
            writer = writer.option("mergeSchema", str(merge_schema).lower())
        for key, value in options.items():
            writer = writer.option(key, value)
        if normalized_mode == "overwrite":
            writer = writer.mode("overwrite").option("overwriteSchema", "true")
            effective_mode = "overwrite"
        elif normalized_mode == "append":
            writer = writer.mode("append")
            effective_mode = "append"
        elif normalized_mode:
            writer = writer.mode(mode)
            effective_mode = normalized_mode
        else:
            writer = writer.mode("append")
            effective_mode = "append"
        _log_managed_table(
            "fabric_save_as_managed_table",
            ref,
            mode=effective_mode,
        )
        writer.saveAsTable(ref.full_table_name)
        return

    if ref.is_lakehouse and ref.kind == "files" and ref.lakehouse:
        resolved = resolve_fabric_files_path(ref)
        effective_mode = normalized_mode or (mode or "append")
        writer = _apply_common_writer(
            df,
            fmt,
            options,
            effective_mode,
            partition_by,
            merge_schema,
        )
        LOGGER.info(
            "Fabric files write",
            extra={
                "event": "fabric_write_files_path",
                "backend": "fabric",
                "lakehouse": ref.lakehouse,
                "resolved_path": resolved,
                "mode": effective_mode,
            },
        )
        writer.save(resolved)
        return

    local.write(
        df,
        uri,
        fmt,
        mode,
        options,
        partition_by=partition_by,
        merge_schema=merge_schema,
    )
