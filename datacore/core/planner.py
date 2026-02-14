"""Planificación de datasets y capas sin dependencia de Spark."""

from __future__ import annotations

from typing import Any

from datacore.platforms.base import PlatformBase

_SENSITIVE_KEYS = {"password", "token", "secret", "key", "connectionstring", "apikey"}


def resolve_references(value: Any, platform: PlatformBase) -> Any:
    """Resuelve referencias ${ENV} y ${SECRET:...} de manera recursiva."""
    if isinstance(value, dict):
        return {key: resolve_references(val, platform) for key, val in value.items()}
    if isinstance(value, list):
        return [resolve_references(item, platform) for item in value]
    if isinstance(value, str):
        return platform.resolve_secret_reference(value)
    return value


def _sanitize_options(options: dict[str, Any] | None) -> dict[str, Any]:
    if not options:
        return {}
    sanitized: dict[str, Any] = {}
    for key, value in options.items():
        lowered = key.lower()
        if any(token in lowered for token in _SENSITIVE_KEYS):
            sanitized[key] = "***"
        else:
            sanitized[key] = value
    return sanitized


def detect_dataset_issues(dataset: dict[str, Any]) -> list[str]:
    """Detecta problemas de configuración que no requieren ejecutar el pipeline."""
    issues: list[str] = []
    incremental_cfg = dataset.get("incremental", {})
    mode = incremental_cfg.get("mode")
    if mode and mode not in {"full", "append", "merge"}:
        issues.append(f"incremental.mode {mode} no soportado")
    if mode == "merge" and not incremental_cfg.get("keys"):
        issues.append("incremental.merge requiere keys definidos")
    if isinstance(dataset.get("source"), list) and not dataset.get("merge_strategy"):
        issues.append("source múltiple requiere merge_strategy para resolver duplicados")

    sink = dataset.get("sink", {})
    if sink.get("type") == "storage" and sink.get("format") not in {
        "delta",
        "parquet",
        "csv",
        "json",
        "avro",
        "orc",
    }:
        issues.append(f"Formato de sink {sink.get('format')} no soportado para storage")

    if mode == "merge" and sink.get("type") not in {"storage", "warehouse", "nosql", "iceberg"}:
        issues.append("incremental.merge requiere un sink de tipo storage/warehouse/nosql/iceberg")

    if sink.get("type") == "iceberg":
        if not sink.get("namespace"):
            issues.append("sink.iceberg requiere namespace definido")
        if not sink.get("table"):
            issues.append("sink.iceberg requiere table definido")

    return issues


def build_dataset_plan(dataset: dict[str, Any]) -> dict[str, Any]:
    """Construye el plan declarativo de un dataset."""
    sources_conf = dataset.get("source")
    if isinstance(sources_conf, list):
        sources = sources_conf
    elif sources_conf:
        sources = [sources_conf]
    else:
        sources = []

    source_summaries = []
    for idx, source in enumerate(sources):
        if not isinstance(source, dict):
            continue
        summary: dict[str, Any] = {
            "type": source.get("type"),
            "format": source.get("format"),
            "uri": source.get("uri"),
        }
        source_id = source.get("id") or source.get("name") or f"src_{idx}"
        summary["id"] = source_id
        if source.get("name"):
            summary["name"] = source.get("name")
        if source.get("table"):
            summary["table"] = source.get("table")
        if source.get("url"):
            summary["url"] = source.get("url")
        if source.get("partitioning"):
            summary["partitioning"] = source.get("partitioning")
        summary["options"] = _sanitize_options(source.get("options"))
        if source.get("read_options"):
            summary["read_options"] = _sanitize_options(source.get("read_options"))
        if source.get("cdc"):
            summary["cdc"] = dict(source.get("cdc"))
        source_summaries.append(summary)

    sink_conf = dataset.get("sink", {})
    sink_summary: dict[str, Any] = {
        "type": sink_conf.get("type"),
        "format": sink_conf.get("format"),
        "uri": sink_conf.get("uri"),
        "engine": sink_conf.get("engine"),
        "table": sink_conf.get("table"),
        "mode": sink_conf.get("mode", "append"),
        "partition_by": sink_conf.get("partition_by"),
        "merge_schema": sink_conf.get("merge_schema") or sink_conf.get("mergeSchema"),
        "compression": sink_conf.get("compression"),
        "coalesce": sink_conf.get("coalesce"),
        "repartition": sink_conf.get("repartition"),
        "target_file_size_mb": sink_conf.get("target_file_size_mb") or sink_conf.get("file_size_mb"),
        "checkpoint_location": sink_conf.get("checkpoint_location"),
        "options": _sanitize_options(sink_conf.get("options")),
    }
    if sink_conf.get("write_options"):
        sink_summary["write_options"] = _sanitize_options(sink_conf.get("write_options"))
    if sink_conf.get("temporary_gcs_bucket") or sink_conf.get("temporaryGcsBucket"):
        sink_summary["temporary_gcs_bucket"] = sink_conf.get("temporary_gcs_bucket") or sink_conf.get(
            "temporaryGcsBucket"
        )
    if sink_conf.get("intermediate_format") or sink_conf.get("intermediateFormat"):
        sink_summary["intermediate_format"] = sink_conf.get("intermediate_format") or sink_conf.get(
            "intermediateFormat"
        )

    transform_cfg = dataset.get("transform", {})
    transform_summary = {
        "sql": transform_cfg.get("sql", []),
        "ops": transform_cfg.get("ops", []),
        "udf": transform_cfg.get("udf", []),
        "add_ingestion_ts": transform_cfg.get("add_ingestion_ts", True),
        "federate": transform_cfg.get("federate"),
    }

    validation_cfg = dataset.get("validation", {})
    streaming_cfg = dataset.get("streaming", {})
    incremental_cfg = dataset.get("incremental", {})

    plan = {
        "name": dataset["name"],
        "layer": dataset.get("layer"),
        "source_plan": {
            "sources": source_summaries,
            "merge_strategy": dataset.get("merge_strategy"),
        },
        "transform_plan": transform_summary,
        "federate_plan": transform_cfg.get("federate"),
        "validation_plan": validation_cfg,
        "incremental_plan": incremental_cfg,
        "streaming_plan": {
            "enabled": streaming_cfg.get("enabled", False),
            "trigger": streaming_cfg.get("trigger"),
            "checkpoint_location": streaming_cfg.get("checkpoint_location") or streaming_cfg.get("checkpoint"),
            "watermark": streaming_cfg.get("watermark") or incremental_cfg.get("watermark"),
        },
        "sink_plan": sink_summary,
        "cache_plan": dataset.get("cache", {}),
        "issues": detect_dataset_issues(dataset),
    }

    return plan
