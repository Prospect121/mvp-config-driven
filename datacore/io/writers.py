"""Escritores genéricos."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame

from datacore.connectors.db import jdbc
from datacore.connectors.storage import abfs, gcs, local, s3
from datacore.platforms.base import PlatformBase


def write_batch(df: DataFrame, platform: PlatformBase, sink: dict[str, Any]) -> None:
    sink_type = sink["type"]
    fmt = sink.get("format", "parquet")
    options = sink.get("options", {})
    mode = sink.get("mode", "append")
    if sink_type == "storage":
        uri = platform.normalize_uri(sink["uri"])
        backend = sink.get("backend", platform.name)
        if backend == "aws":
            s3.write(df, uri, fmt, mode, options)
            return
        if backend == "azure":
            abfs.write(df, uri, fmt, mode, options)
            return
        if backend == "gcp":
            gcs.write(df, uri, fmt, mode, options)
            return
        local.write(df, uri, fmt, mode, options)
        return
    if sink_type == "warehouse":
        if sink.get("engine") in {"synapse", "redshift", "postgres", "mysql", "sqlserver"}:
            jdbc.write(df, sink)
            return
        if sink.get("engine") == "bigquery":
            writer = df.write.format("bigquery").mode(mode)
            for key, value in options.items():
                writer = writer.option(key, value)
            writer.save(sink["table"])
            return
    raise ValueError(f"Tipo de destino no soportado: {sink_type}")


def write_stream(
    df: DataFrame,
    platform: PlatformBase,
    sink: dict[str, Any],
    checkpoint: str,
) -> None:
    sink_type = sink["type"]
    fmt = sink.get("format", "parquet")
    options = {**sink.get("options", {}), "checkpointLocation": checkpoint}
    mode = sink.get("mode", "append")
    if sink_type == "storage":
        uri = platform.normalize_uri(sink["uri"])
        query = df.writeStream.outputMode(mode).format(fmt).options(**options).start(uri)
        query.awaitTermination(sink.get("await_termination", False))
        return
    raise ValueError(f"Streaming no soportado para {sink_type}")
