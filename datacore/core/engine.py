"""Motor de ejecución por capas con planificación y validaciones enriquecidas."""

from __future__ import annotations

import json
from hashlib import sha1
from pathlib import Path
from datetime import datetime
from time import perf_counter
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from datacore.core import cache, federation, pushdown, transforms, validation
from datacore.core.cdc import incremental_fetch_jdbc
from datacore.core.incremental import prepare_incremental
from datacore.io import readers, writers
from datacore.platforms.aws_glue import AwsGluePlatform
from datacore.platforms.azure_databricks import AzureDatabricksPlatform
from datacore.platforms.base import LocalPlatform, PlatformBase
from datacore.platforms.fabric import FabricPlatform
from datacore.platforms.gcp_dataproc import GcpDataprocPlatform
from datacore.utils.logging import get_logger
from datacore.utils import observability

LOGGER = get_logger(__name__)

PLATFORM_MAP = {
    "azure": AzureDatabricksPlatform,
    "aws": AwsGluePlatform,
    "gcp": GcpDataprocPlatform,
    "fabric": FabricPlatform,
    "local": LocalPlatform,
}

_SENSITIVE_KEYS = {"password", "token", "secret", "key", "connectionstring", "apikey"}


def _resolve_platform(name: str | None, config: dict[str, Any]) -> PlatformBase:
    platform_cls = PLATFORM_MAP.get(name or config.get("platform", "local"))
    if not platform_cls:
        raise ValueError(f"Plataforma no soportada: {name}")
    return platform_cls(config=config)


def _prepare_spark(platform: PlatformBase, config: dict[str, Any]):
    spark_conf: dict[str, Any] = {}
    spark_section = config.get("spark", {})
    if "shuffle_partitions" in spark_section:
        spark_conf["spark.sql.shuffle.partitions"] = spark_section["shuffle_partitions"]
    if "extra_conf" in spark_section:
        spark_conf.update(spark_section["extra_conf"])
    active = SparkSession.getActiveSession()
    if active is not None and getattr(platform, "reuse_active_session", True):
        LOGGER.info(
            "Reusing active SparkSession before building platform session",
            extra={"platform": platform.name, "event": "spark_session_reuse"},
        )
        return active

    session = platform.build_spark_session(spark_conf)
    LOGGER.info(
        "SparkSession prepared",
        extra={"platform": platform.name, "event": "spark_session_ready"},
    )
    return session


def _resolve_references(value: Any, platform: PlatformBase):
    if isinstance(value, dict):
        return {key: _resolve_references(val, platform) for key, val in value.items()}
    if isinstance(value, list):
        return [_resolve_references(item, platform) for item in value]
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


def _cdc_state_path(
    platform: PlatformBase,
    layer: str,
    dataset: str,
    environment: str,
    source_id: str,
) -> Path:
    base = Path(platform.checkpoint_dir(layer, dataset, environment)) / "_cdc"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{source_id}.json"


def _load_cdc_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _store_cdc_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _apply_transformations(df: DataFrame, transform_config: dict[str, Any]) -> DataFrame:
    sql_steps = transform_config.get("sql", [])
    for idx, statement in enumerate(sql_steps):
        view_name = f"_src_{idx}"
        df.createOrReplaceTempView(view_name)
        # También crear vista _src como alias conveniente (el primero o el más reciente)
        df.createOrReplaceTempView("_src")
        df = df.sparkSession.sql(statement)
    add_ingestion = transform_config.get("add_ingestion_ts", True)
    if add_ingestion and "_ingestion_ts" not in df.columns:
        df = df.withColumn("_ingestion_ts", F.current_timestamp())
    udf_steps = transform_config.get("udf", [])
    if udf_steps:
        df = transforms.apply_registered(df, udf_steps)
    ops_steps = transform_config.get("ops", [])
    if ops_steps:
        df = transforms.apply_ops(df, ops_steps)
    if add_ingestion and "_ingestion_ts" not in df.columns:
        df = df.withColumn("_ingestion_ts", F.current_timestamp())
    return df


def _merge_strategy(df: DataFrame, strategy: dict[str, Any]) -> DataFrame:
    keys = strategy.get("keys")
    if not keys:
        raise ValueError("merge_strategy requiere keys definidos")
    prefer = strategy.get("prefer", "newest")
    order_by = strategy.get("order_by", ["_ingestion_ts DESC"])
    if prefer == "coalesce":
        non_keys = [col for col in df.columns if col not in keys and col != "__dc_source_ordinal"]
        aggregations = [F.first(F.col(col), ignorenulls=True).alias(col) for col in non_keys]
        return df.groupBy(*keys).agg(*aggregations)
    if prefer == "left":
        order_by = ["__dc_source_ordinal ASC", *order_by]
    window = Window.partitionBy(*[F.col(key) for key in keys]).orderBy(
        *[F.expr(expr) for expr in order_by]
    )
    ranked = df.withColumn("__dc_union_rank", F.row_number().over(window))
    return ranked.filter(F.col("__dc_union_rank") == 1).drop("__dc_union_rank")


def _source_list(dataset: dict[str, Any]) -> list[dict[str, Any]]:
    raw = dataset.get("source")
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, dict):
        return [raw]
    raise ValueError("La definición de source es obligatoria")


def _source_identifier(idx: int, source: dict[str, Any]) -> str:
    return str(source.get("id") or source.get("name") or f"src_{idx}")


def _prepare_source_for_pushdown(
    source: dict[str, Any],
    transform_cfg: dict[str, Any],
) -> dict[str, Any]:
    ops_cfg = transform_cfg.get("ops", [])
    sql = pushdown.try_pushdown(source, ops_cfg)
    if not sql:
        return source
    updated = dict(source)
    updated.pop("table", None)
    updated["query"] = sql
    return updated


def _read_single_source(
    spark,
    platform: PlatformBase,
    dataset: dict[str, Any],
    source: dict[str, Any],
    *,
    source_id: str,
    ordinal: int,
    layer: str,
    environment: str,
) -> DataFrame:
    transform_cfg = dataset.get("transform", {})
    prepared = _prepare_source_for_pushdown(source, transform_cfg)
    cdc_cfg = prepared.pop("cdc", None)
    if cdc_cfg and prepared.get("type") == "jdbc":
        state_path = _cdc_state_path(platform, layer, dataset["name"], environment, source_id)
        previous_state = _load_cdc_state(state_path)
        df, new_state = incremental_fetch_jdbc(spark, prepared, cdc_cfg, previous_state)
        _store_cdc_state(state_path, new_state)
    else:
        df = readers.read_batch(
            spark,
            platform,
            prepared,
            layer=layer,
            dataset=dataset["name"],
            environment=environment,
        )
    df = df.withColumn("__dc_source_ordinal", F.lit(ordinal))
    view_name = f"_src_{source_id}"
    df.createOrReplaceTempView(view_name)
    return df


def _combine_sources(
    dataset: dict[str, Any],
    sources: dict[str, DataFrame],
) -> DataFrame:
    transform_cfg = dataset.get("transform", {})
    federate_cfg = transform_cfg.get("federate")
    if federate_cfg:
        sanitized = {
            name: (df.drop("__dc_source_ordinal") if "__dc_source_ordinal" in df.columns else df)
            for name, df in sources.items()
        }
        return federation.apply_federation(sanitized, federate_cfg)
    frames = list(sources.values())
    if not frames:
        raise ValueError("No se encontraron fuentes para el dataset")
    if len(frames) == 1:
        combined = frames[0]
        return (
            combined.drop("__dc_source_ordinal")
            if "__dc_source_ordinal" in combined.columns
            else combined
        )
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    if dataset.get("merge_strategy"):
        combined = _merge_strategy(combined, dataset["merge_strategy"])
    return (
        combined.drop("__dc_source_ordinal")
        if "__dc_source_ordinal" in combined.columns
        else combined
    )


def _build_sources_map(
    spark,
    platform: PlatformBase,
    dataset: dict[str, Any],
    *,
    layer: str,
    environment: str,
) -> dict[str, DataFrame]:
    frames: list[tuple[str, DataFrame]] = []
    for idx, source in enumerate(_source_list(dataset)):
        source_id = _source_identifier(idx, source)
        df = _read_single_source(
            spark,
            platform,
            dataset,
            dict(source),
            source_id=source_id,
            ordinal=idx,
            layer=layer,
            environment=environment,
        )
        frames.append((source_id, df))
    return dict(frames)


def _dataset_signature(dataset: dict[str, Any]) -> str:
    payload = {
        "source": dataset.get("source"),
        "transform": dataset.get("transform"),
        "incremental": dataset.get("incremental"),
    }
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return sha1(serialized.encode("utf-8")).hexdigest()


def _detect_dataset_issues(dataset: dict[str, Any]) -> list[str]:
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
    if mode == "merge" and sink.get("type") not in {"storage", "warehouse", "nosql"}:
        issues.append("incremental.merge requiere un sink de tipo storage/warehouse/nosql")
    return issues


def _build_plan(dataset: dict[str, Any]) -> dict[str, Any]:
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
        summary = {
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
    sink_summary = {
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
        "target_file_size_mb": sink_conf.get("target_file_size_mb")
        or sink_conf.get("file_size_mb"),
        "checkpoint_location": sink_conf.get("checkpoint_location"),
        "options": _sanitize_options(sink_conf.get("options")),
    }
    if sink_conf.get("write_options"):
        sink_summary["write_options"] = _sanitize_options(sink_conf.get("write_options"))
    if sink_conf.get("temporary_gcs_bucket") or sink_conf.get("temporaryGcsBucket"):
        sink_summary["temporary_gcs_bucket"] = sink_conf.get(
            "temporary_gcs_bucket"
        ) or sink_conf.get("temporaryGcsBucket")
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
            "checkpoint_location": streaming_cfg.get("checkpoint_location")
            or streaming_cfg.get("checkpoint"),
            "watermark": streaming_cfg.get("watermark") or incremental_cfg.get("watermark"),
        },
        "sink_plan": sink_summary,
        "cache_plan": dataset.get("cache", {}),
    }
    plan["issues"] = _detect_dataset_issues(dataset)
    return plan


def _apply_streaming_options(df: DataFrame, dataset: dict[str, Any]) -> DataFrame:
    streaming_cfg = dataset.get("streaming", {})
    watermark_cfg = streaming_cfg.get("watermark")
    incremental_cfg = dataset.get("incremental", {})
    if not watermark_cfg and incremental_cfg.get("watermark"):
        watermark_cfg = incremental_cfg["watermark"]
    if not watermark_cfg:
        column = streaming_cfg.get("watermark_column") or incremental_cfg.get("watermark_column")
        delay = streaming_cfg.get("watermark") or incremental_cfg.get("watermark")
        if column and delay:
            watermark_cfg = {"column": column, "delay_threshold": delay}
    if watermark_cfg and watermark_cfg.get("column") and watermark_cfg.get("delay_threshold"):
        df = df.withWatermark(watermark_cfg["column"], watermark_cfg["delay_threshold"])
    return df


def _handle_batch_dataset(
    layer: str,
    dataset: dict[str, Any],
    platform: PlatformBase,
    environment: str,
    spark,
    run_id: str,
) -> dict[str, Any]:
    sources = _build_sources_map(
        spark,
        platform,
        dataset,
        layer=layer,
        environment=environment,
    )
    combined = _combine_sources(dataset, sources)
    transformed = _apply_transformations(combined, dataset.get("transform", {}))
    cache_cfg = dataset.get("cache")
    if cache_cfg:
        transformed = cache.with_cache(
            transformed,
            cache_cfg,
            {
                "dataset": dataset["name"],
                "layer": layer,
                "environment": environment,
                "signature": _dataset_signature(dataset),
            },
        )
    validation_cfg = dataset.get("validation", {})
    validation_result = validation.apply_validation(transformed, validation_cfg)
    metrics = dict(validation_result.metrics)
    if cache_cfg:
        metrics["cache_enabled"] = bool(cache_cfg.get("enabled"))
    metrics.update(
        {
            "dataset": dataset["name"],
            "layer": layer,
            "environment": environment,
            "run_id": run_id,
            "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds"),
        }
    )

    invalid_rows = metrics.get("invalid_rows", 0)
    if invalid_rows:
        writers.write_rejects(
            validation_result.invalid_df,
            platform,
            dataset["sink"],
            layer=layer,
            dataset=dataset["name"],
            environment=environment,
        )

    quarantine_sink = (validation_result.extras or {}).get("quarantine_sink")
    quarantine_rows = 0
    if quarantine_sink is None:
        quarantine_sink = validation_cfg.get("quarantine_sink")
    if quarantine_sink:
        quarantine_rows = validation_result.quarantine_df.count()
        metrics["quarantine_rows"] = quarantine_rows
        if quarantine_rows > 0:
            writers.write_batch(validation_result.quarantine_df, platform, quarantine_sink)
    else:
        metrics["quarantine_rows"] = quarantine_rows

    incremental_cfg = dataset.get("incremental")
    valid_df, sink_to_use, handled = prepare_incremental(
        validation_result.valid_df,
        dataset["sink"],
        incremental_cfg,
        platform,
    )
    if not handled:
        writers.write_batch(valid_df, platform, sink_to_use)

    # Permitir desactivar la escritura de métricas por configuración del sink
    sink_cfg = dataset.get("sink", {})
    if sink_cfg.get("metrics_enabled", True):
        writers.write_metrics(
            spark,
            platform,
            sink_cfg,
            layer=layer,
            dataset=dataset["name"],
            environment=environment,
            run_id=run_id,
            metrics=metrics,
        )

    return {
        "name": dataset["name"],
        "status": "completed",
        "metrics": metrics,
        "quarantine_rows": quarantine_rows,
    }


def _handle_streaming_dataset(
    layer: str,
    dataset: dict[str, Any],
    platform: PlatformBase,
    environment: str,
    spark,
    run_id: str,
) -> dict[str, Any]:
    df_stream = readers.read_stream(
        spark,
        platform,
        dataset["source"],
        layer=layer,
        dataset=dataset["name"],
        environment=environment,
    )
    df_stream = _apply_streaming_options(df_stream, dataset)
    transformed = _apply_transformations(df_stream, dataset.get("transform", {}))
    streaming_cfg = dataset.get("streaming", {})
    sink_cfg = dataset.get("sink", {})
    checkpoint = (
        streaming_cfg.get("checkpoint_location")
        or streaming_cfg.get("checkpoint")
        or sink_cfg.get("checkpoint_location")
        or platform.checkpoint_dir(layer, dataset["name"], environment)
    )
    trigger = streaming_cfg.get("trigger")
    writers.write_stream(transformed, platform, sink_cfg, checkpoint, trigger=trigger)
    return {"name": dataset["name"], "status": "streaming", "run_id": run_id}


def _process_dataset(
    layer: str,
    dataset: dict[str, Any],
    platform: PlatformBase,
    environment: str,
    spark,
    run_id: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    dataset_cfg = _resolve_references(dataset, platform)
    LOGGER.info(
        "Procesando dataset",
        extra={
            "dataset": dataset_cfg["name"],
            "run_id": run_id,
            "layer": layer,
        },
    )
    if dry_run:
        plan = _build_plan(dataset_cfg)
        plan["status"] = "planned"
        plan["run_id"] = run_id
        return plan
    streaming_cfg = dataset_cfg.get("streaming", {})
    start_time = perf_counter()
    try:
        if streaming_cfg.get("enabled"):
            result = _handle_streaming_dataset(
                layer, dataset_cfg, platform, environment, spark, run_id
            )
        else:
            result = _handle_batch_dataset(layer, dataset_cfg, platform, environment, spark, run_id)
        duration = perf_counter() - start_time
        result.setdefault("run_id", run_id)
        result["duration_seconds"] = duration
        observability.record_dataset(
            dataset_cfg["name"],
            result.get("status", "completed"),
            duration,
            result.get("metrics"),
        )
        lineage_cfg = dataset_cfg.get("lineage")
        if lineage_cfg:
            observability.emit_openlineage(
                {
                    "dataset": dataset_cfg["name"],
                    "layer": layer,
                    "status": result.get("status"),
                    "run_id": run_id,
                    "duration": duration,
                    "metrics": result.get("metrics"),
                },
                lineage_cfg,
            )
        return result
    except Exception:
        duration = perf_counter() - start_time
        observability.record_dataset(dataset_cfg["name"], "failed", duration, None)
        raise


def run_layer_plan(
    layer: str,
    config: dict[str, Any],
    platform_name: str | None = None,
    environment: str | None = None,
    dry_run: bool = False,
    fail_fast: bool = False,
) -> dict[str, Any]:
    platform = _resolve_platform(platform_name, config)
    spark = _prepare_spark(platform, config)
    results: list[dict[str, Any]] = []
    env = environment or config.get("environment", "dev")
    run_id = observability.new_run_id()
    for dataset in config.get("datasets", []):
        if dataset.get("layer") != layer:
            continue
        try:
            results.append(
                _process_dataset(
                    layer,
                    dataset,
                    platform,
                    env,
                    spark,
                    run_id,
                    dry_run=dry_run,
                )
            )
        except Exception as exc:  # pragma: no cover - control de fallos
            LOGGER.error("Fallo en dataset %s: %s", dataset.get("name"), exc)
            if fail_fast:
                raise
            results.append(
                {
                    "name": dataset.get("name"),
                    "status": "failed",
                    "error": str(exc),
                    "run_id": run_id,
                    "duration_seconds": None,
                }
            )
    created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    LOGGER.info(
        "Ejecución de capa completada",
        extra={"layer": layer, "run_id": run_id, "datasets": len(results)},
    )
    return {"run_id": run_id, "created_at": created_at, "datasets": results}
