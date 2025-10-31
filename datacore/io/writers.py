"""Escritores genéricos y especializados."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

try:  # pragma: no cover - dependencia opcional
    import boto3
except ImportError:  # pragma: no cover - entornos sin boto3
    boto3 = None  # type: ignore[assignment]

from pyspark.sql import DataFrame

from datacore.connectors.db import jdbc
from datacore.connectors.storage import abfs, gcs, local, s3
from datacore.platforms.base import PlatformBase
from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)


def _storage_connector(backend: str):
    connectors = {
        "aws": s3,
        "azure": abfs,
        "gcp": gcs,
        "local": local,
    }
    return connectors.get(backend, local)


def _merge_schema_flag(sink: dict[str, Any]) -> bool | None:
    if "mergeSchema" in sink:
        return bool(sink["mergeSchema"])
    if "merge_schema" in sink:
        return bool(sink["merge_schema"])
    return None


def _merge_write_options(sink: dict[str, Any]) -> dict[str, Any]:
    options = {**sink.get("options", {})}
    options.update(sink.get("write_options", {}))
    if "compression" in sink and "compression" not in options:
        options["compression"] = sink["compression"]
    if sink.get("atomic") is not None and "atomic" not in options:
        options["atomic"] = sink["atomic"]
    return options


def _apply_partitioning(df: DataFrame, sink: dict[str, Any]) -> DataFrame:
    result = df
    if "coalesce" in sink:
        result = result.coalesce(int(sink["coalesce"]))
    repartition = sink.get("repartition")
    if repartition:
        if isinstance(repartition, int):
            result = result.repartition(repartition)
        elif isinstance(repartition, (list, tuple)):
            result = result.repartition(*repartition)
        elif isinstance(repartition, dict):
            num = repartition.get("num") or repartition.get("partitions")
            cols = repartition.get("cols") or repartition.get("columns") or []
            if num and cols:
                result = result.repartition(int(num), *cols)
            elif num:
                result = result.repartition(int(num))
            elif cols:
                result = result.repartition(*cols)
    return result


def write_batch(df: DataFrame, platform: PlatformBase, sink: dict[str, Any]) -> None:
    sink_type = sink["type"]
    fmt = sink.get("format", "parquet")
    mode = sink.get("mode", "append")
    options = _merge_write_options(sink)
    partition_by = sink.get("partition_by")
    merge_schema = _merge_schema_flag(sink)
    prepared_df = _apply_partitioning(df, sink)
    file_size = sink.get("target_file_size_mb", sink.get("file_size_mb"))
    if file_size is not None:
        options.setdefault("maxRecordsPerFile", int(file_size) * 1024 * 1024)

    if sink_type == "storage":
        uri = platform.normalize_uri(sink["uri"])
        backend = sink.get("backend", platform.name)
        connector = _storage_connector(backend)
        connector.write(
            prepared_df,
            uri,
            fmt,
            mode,
            options,
            partition_by=partition_by,
            merge_schema=merge_schema,
        )
        return

    if sink_type == "warehouse":
        engine = sink.get("engine")
        if engine in {"synapse", "redshift", "postgres", "mysql", "sqlserver"}:
            jdbc.write(prepared_df, {**sink, "options": options})
            return
        if engine == "bigquery":
            writer = prepared_df.write.format("bigquery").mode(mode)
            for key, value in options.items():
                writer = writer.option(key, value)
            temp_bucket = sink.get("temporary_gcs_bucket") or sink.get("temporaryGcsBucket")
            if temp_bucket:
                writer = writer.option("temporaryGcsBucket", temp_bucket)
            intermediate = sink.get("intermediate_format") or sink.get("intermediateFormat")
            if intermediate:
                writer = writer.option("intermediateFormat", intermediate)
            writer.save(sink["table"])
            return
        raise ValueError(f"Motor de warehouse no soportado: {engine}")

    if sink_type == "nosql":
        _write_nosql(prepared_df, sink)
        return

    raise ValueError(f"Tipo de destino no soportado: {sink_type}")


def _write_nosql(df: DataFrame, sink: dict[str, Any]) -> None:
    engine = sink.get("engine")
    options = _merge_write_options(sink)
    if engine == "cosmosdb":
        writer = df.write.format(options.get("format", "cosmos.oltp")).mode(sink.get("mode", "append"))
        for key, value in options.items():
            writer = writer.option(key, value)
        writer.save()
        return
    if engine == "dynamodb":
        _write_dynamodb(df, sink, options)
        return
    raise ValueError(f"Motor NoSQL no soportado: {engine}")


def _write_dynamodb(df: DataFrame, sink: dict[str, Any], options: dict[str, Any]) -> None:
    if boto3 is None:  # pragma: no cover - requiere dependencia externa
        raise RuntimeError("boto3 es requerido para escribir en DynamoDB")
    table_name = sink["table"]
    region = options.get("region")
    resource = boto3.resource("dynamodb", region_name=region)
    table = resource.Table(table_name)
    batch_size = int(sink.get("batch_size", 25))
    if sink.get("glue_catalog"):
        LOGGER.info(
            "Actualización de Glue Catalog solicitada para %s, no-op en ejecución local",
            sink["glue_catalog"],
        )

    items = (row.asDict(recursive=True) for row in df.toLocalIterator())
    buffer: list[dict[str, Any]] = []
    with table.batch_writer(overwrite_by_pkeys=sink.get("keys")) as batch:
        for item in items:
            buffer.append(item)
            if len(buffer) >= batch_size:
                for chunk in buffer:
                    batch.put_item(Item=chunk)
                buffer.clear()
        for chunk in buffer:
            batch.put_item(Item=chunk)


def _stream_writer_common(
    df: DataFrame,
    options: dict[str, Any],
    fmt: str,
    mode: str,
    checkpoint: str,
    trigger: str | None = None,
) -> None:
    stream_options = {k: v for k, v in options.items() if k != "await_termination"}
    await_termination = bool(options.get("await_termination", False))
    writer = df.writeStream.format(fmt).outputMode(mode).options(**stream_options).option(
        "checkpointLocation", checkpoint
    )
    if trigger:
        writer = writer.trigger(processingTime=trigger)
    query = writer.start()
    query.awaitTermination(await_termination)


def write_stream(
    df: DataFrame,
    platform: PlatformBase,
    sink: dict[str, Any],
    checkpoint: str,
    trigger: str | None = None,
) -> None:
    sink_type = sink["type"]
    mode = sink.get("mode", "append")
    options = _merge_write_options(sink)

    if sink_type == "storage":
        fmt = sink.get("format", "parquet")
        uri = platform.normalize_uri(sink["uri"])
        options.setdefault("path", uri)
        _stream_writer_common(df, options, fmt, mode, checkpoint, trigger)
        return

    if sink_type == "kafka":
        options.setdefault("topic", sink.get("topic"))
        _stream_writer_common(df, options, "kafka", mode, checkpoint, trigger)
        return

    if sink_type == "event_hubs":
        if "eventHubs.connectionString" not in options:
            options["eventHubs.connectionString"] = sink.get("connectionString")
        _stream_writer_common(df, options, "eventhubs", mode, checkpoint, trigger)
        return

    raise ValueError(f"Streaming no soportado para {sink_type}")


def write_rejects(
    df: DataFrame,
    platform: PlatformBase,
    sink: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
    fmt: str = "parquet",
) -> None:
    if "uri" not in sink:
        LOGGER.warning("No se pueden almacenar rejects para un sink sin URI explícita")
        return
    base_uri = sink["uri"].rstrip("/")
    rejects_uri = f"{base_uri}/_rejects"
    backend = sink.get("backend", platform.name)
    connector = _storage_connector(backend)
    options = sink.get("reject_options", {})
    connector.write(
        df,
        platform.normalize_uri(rejects_uri),
        fmt,
        "append",
        options,
        partition_by=None,
        merge_schema=True,
    )


def write_metrics(
    spark,
    platform: PlatformBase,
    sink: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
    run_id: str,
    metrics: dict[str, Any],
) -> None:
    if "uri" not in sink:
        LOGGER.warning("No se pueden almacenar métricas para un sink sin URI explícita")
        return
    base_uri = sink["uri"].rstrip("/")
    metrics_uri = f"{base_uri}/_metrics/{run_id}.json"
    backend = sink.get("backend", platform.name)
    connector = _storage_connector(backend)
    metrics_json = json.dumps(metrics, ensure_ascii=False)
    metrics_df = spark.read.json(spark.sparkContext.parallelize([metrics_json]))
    connector.write(
        metrics_df,
        platform.normalize_uri(metrics_uri),
        "json",
        "overwrite",
        {"compression": sink.get("metrics_compression", "none")},
        partition_by=None,
        merge_schema=True,
    )
