"""Lectores genéricos para múltiples orígenes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from datacore.connectors.api import graphql, rest
from datacore.connectors import http
from datacore.connectors.db import jdbc
from datacore.connectors.storage import abfs, gcs, local, s3
from datacore.platforms.base import PlatformBase

FORMAT_DEFAULTS: dict[str, dict[str, Any]] = {
    "csv": {"header": "true", "inferSchema": "true", "delimiter": ","},
    "json": {"multiLine": "true"},
    "parquet": {},
    "avro": {},
    "orc": {},
}


def _apply_options(reader, options: dict[str, Any]):
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader


def _schema_cache_path(
    platform: PlatformBase, layer: str, dataset: str, environment: str
) -> Path:
    checkpoint = Path(platform.checkpoint_dir(layer, dataset, environment))
    cache_dir = checkpoint / "_schema_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "schema.json"


def _load_schema(cache_path: Path) -> StructType | None:
    if not cache_path.exists():
        return None
    with cache_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return StructType.fromJson(payload)


def _store_schema(cache_path: Path, schema: StructType) -> None:
    cache_path.write_text(
        json.dumps(schema.jsonValue(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _merge_format_options(fmt: str, options: dict[str, Any], infer_schema: bool) -> dict[str, Any]:
    defaults = FORMAT_DEFAULTS.get(fmt, {})
    merged = {**defaults, **options}
    if infer_schema and fmt == "csv":
        merged.setdefault("inferSchema", "true")
    return merged


def _storage_connector(backend: str):
    connectors = {
        "aws": s3,
        "azure": abfs,
        "gcp": gcs,
        "local": local,
    }
    return connectors.get(backend, local)


def _merge_read_options(source: dict[str, Any]) -> dict[str, Any]:
    return {**source.get("options", {}), **source.get("read_options", {})}


def _read_storage(
    spark: SparkSession,
    platform: PlatformBase,
    source: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
) -> DataFrame:
    fmt = source.get("format", "parquet")
    options = _merge_read_options(source)
    infer_schema = bool(source.get("infer_schema"))
    merged_options = _merge_format_options(fmt, options, infer_schema)
    backend = source.get("backend", platform.name)
    uri = platform.normalize_uri(source["uri"])
    cache_path = _schema_cache_path(platform, layer, dataset, environment)
    cached_schema = _load_schema(cache_path) if infer_schema else None
    connector = _storage_connector(backend)
    df = connector.read(spark, uri, fmt, merged_options, schema=cached_schema)
    if infer_schema and cached_schema is None:
        _store_schema(cache_path, df.schema)
    return df


def _record_path_to_list(record_path: str | list[str] | None) -> list[str] | None:
    if record_path is None:
        return None
    if isinstance(record_path, list):
        return record_path
    return [segment for segment in record_path.split(".") if segment]


def _extract_records(payload: Any, record_path: list[str] | None) -> list[dict[str, Any]]:
    target = payload
    if record_path:
        for segment in record_path:
            if isinstance(target, dict):
                target = target.get(segment)
            else:
                target = None
            if target is None:
                return []
    if isinstance(target, list):
        return [item for item in target if isinstance(item, dict)]
    if isinstance(target, dict):
        return [target]
    return []


def _flatten_record(record: dict[str, Any], depth: int | None = None, prefix: str = "") -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in record.items():
        column = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        if isinstance(value, dict) and (depth is None or depth > 0):
            nested = _flatten_record(value, None if depth is None else depth - 1, column)
            flattened.update(nested)
        else:
            flattened[column] = value
    return flattened


def _records_to_df(spark: SparkSession, records: list[dict[str, Any]]) -> DataFrame:
    if not records:
        return spark.createDataFrame([], StructType([]))
    return spark.createDataFrame(records)


def _read_api_rest(
    spark: SparkSession,
    platform: PlatformBase,
    source: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
) -> DataFrame:
    fetch_config = {
        **source.get("options", {}),
        **source.get("read_options", {}),
        **{
            key: value
            for key, value in source.items()
            if key
            in {
                "url",
                "headers",
                "params",
                "page_param",
                "start_page",
                "max_pages",
                "backoff",
                "bearer_token",
                "pagination",
                "method",
                "body",
                "auth",
                "retry",
            }
        },
    }
    record_path = _record_path_to_list(source.get("record_path"))
    flatten = source.get("flatten", True)
    depth = source.get("flatten_depth")
    rows: list[dict[str, Any]] = []
    for page in rest.fetch_pages(fetch_config):
        for record in _extract_records(page, record_path):
            rows.append(_flatten_record(record, depth=depth) if flatten else record)
    df = _records_to_df(spark, rows)
    if source.get("infer_schema"):
        cache_path = _schema_cache_path(platform, layer, dataset, environment)
        _store_schema(cache_path, df.schema)
    return df


def _read_api_graphql(
    spark: SparkSession,
    platform: PlatformBase,
    source: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
) -> DataFrame:
    payload = graphql.execute_query(
        {
            "url": source["url"],
            "query": source["query"],
            "variables": source.get("variables", {}),
            "headers": source.get("headers", {}),
        }
    )
    record_path = _record_path_to_list(source.get("record_path"))
    flatten = source.get("flatten", True)
    depth = source.get("flatten_depth")
    rows = [
        _flatten_record(record, depth=depth) if flatten else record
        for record in _extract_records(payload, record_path)
    ]
    df = _records_to_df(spark, rows)
    if source.get("infer_schema"):
        cache_path = _schema_cache_path(platform, layer, dataset, environment)
        _store_schema(cache_path, df.schema)
    return df


def _read_http_endpoint(
    spark: SparkSession,
    platform: PlatformBase,
    source: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
) -> DataFrame:
    fetch_conf = {
        **source,
        "options": {
            **source.get("options", {}),
            **source.get("read_options", {}),
        },
    }
    pages = http.fetch_pages(fetch_conf)
    record_path = _record_path_to_list(source.get("record_path"))
    flatten = source.get("flatten", True)
    depth = source.get("flatten_depth")
    rows: list[dict[str, Any]] = []
    for page in pages:
        for record in _extract_records(page, record_path):
            rows.append(_flatten_record(record, depth=depth) if flatten else record)
    df = _records_to_df(spark, rows)
    if source.get("infer_schema"):
        cache_path = _schema_cache_path(platform, layer, dataset, environment)
        _store_schema(cache_path, df.schema)
    return df


def _parse_stream_payload(df: DataFrame, source: dict[str, Any]) -> DataFrame:
    payload_format = source.get("payload_format", "json").lower()
    schema_conf = source.get("schema")
    schema: StructType | None = None
    if isinstance(schema_conf, dict):
        schema = StructType.fromJson(schema_conf)
    elif isinstance(schema_conf, str):
        try:
            schema = StructType.fromJson(json.loads(schema_conf))
        except json.JSONDecodeError:
            schema = StructType.fromDDL(schema_conf)
    value_col = F.col("value").cast("string")
    if payload_format == "json" and schema is not None:
        parsed = F.from_json(value_col, schema).alias("payload")
        return df.withColumn("payload", parsed).select("payload.*")
    if payload_format == "csv" and schema is not None:
        parsed = F.from_csv(value_col, schema).alias("payload")
        return df.withColumn("payload", parsed).select("payload.*")
    return df.withColumn("payload", value_col)


def _read_kafka_batch(spark: SparkSession, source: dict[str, Any]) -> DataFrame:
    reader = spark.read.format("kafka")
    reader = _apply_options(reader, _merge_read_options(source))
    df = reader.load()
    return _parse_stream_payload(df, source)


def _read_event_hubs_batch(spark: SparkSession, source: dict[str, Any]) -> DataFrame:
    reader = spark.read.format("eventhubs")
    reader = _apply_options(reader, _merge_read_options(source))
    df = reader.load()
    return _parse_stream_payload(df, source)


def read_batch(
    spark: SparkSession,
    platform: PlatformBase,
    source: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
) -> DataFrame:
    source_type = source["type"]
    if source_type == "storage":
        return _read_storage(spark, platform, source, layer=layer, dataset=dataset, environment=environment)
    if source_type == "jdbc":
        return jdbc.read(spark, source)
    if source_type == "api_rest":
        return _read_api_rest(spark, platform, source, layer=layer, dataset=dataset, environment=environment)
    if source_type == "api_graphql":
        return _read_api_graphql(spark, platform, source, layer=layer, dataset=dataset, environment=environment)
    if source_type == "endpoint":
        return _read_http_endpoint(
            spark,
            platform,
            source,
            layer=layer,
            dataset=dataset,
            environment=environment,
        )
    if source_type == "kafka":
        return _read_kafka_batch(spark, source)
    if source_type == "event_hubs":
        return _read_event_hubs_batch(spark, source)
    raise ValueError(f"Tipo de origen no soportado: {source_type}")


def _read_storage_stream(
    spark: SparkSession,
    platform: PlatformBase,
    source: dict[str, Any],
) -> DataFrame:
    fmt = source.get("format", "parquet")
    options = _merge_format_options(fmt, _merge_read_options(source), False)
    uri = platform.normalize_uri(source["uri"])
    reader = spark.readStream.format(fmt)
    reader = _apply_options(reader, options)
    return reader.load(uri)


def _read_kafka_stream(spark: SparkSession, source: dict[str, Any]) -> DataFrame:
    reader = spark.readStream.format("kafka")
    reader = _apply_options(reader, _merge_read_options(source))
    df = reader.load()
    return _parse_stream_payload(df, source)


def _read_event_hubs_stream(spark: SparkSession, source: dict[str, Any]) -> DataFrame:
    reader = spark.readStream.format("eventhubs")
    reader = _apply_options(reader, _merge_read_options(source))
    df = reader.load()
    return _parse_stream_payload(df, source)


def read_stream(
    spark: SparkSession,
    platform: PlatformBase,
    source: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
) -> DataFrame:
    source_type = source["type"]
    if source_type == "storage":
        return _read_storage_stream(spark, platform, source)
    if source_type == "kafka":
        return _read_kafka_stream(spark, source)
    if source_type == "event_hubs":
        return _read_event_hubs_stream(spark, source)
    raise ValueError(f"Streaming no soportado para {source_type}")
