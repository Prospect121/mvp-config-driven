"""Motor de ejecución por capas con planificación y validaciones enriquecidas."""

from __future__ import annotations

import copy
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from datacore.core import transforms, validation
from datacore.core.incremental import handle_incremental
from datacore.io import readers, writers
from datacore.platforms.aws_glue import AwsGluePlatform
from datacore.platforms.azure_databricks import AzureDatabricksPlatform
from datacore.platforms.base import LocalPlatform, PlatformBase
from datacore.platforms.gcp_dataproc import GcpDataprocPlatform
from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)

PLATFORM_MAP = {
    "azure": AzureDatabricksPlatform,
    "aws": AwsGluePlatform,
    "gcp": GcpDataprocPlatform,
    "local": LocalPlatform,
}


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
    return platform.build_spark_session(spark_conf)


def _resolve_references(value: Any, platform: PlatformBase):
    if isinstance(value, dict):
        return {key: _resolve_references(val, platform) for key, val in value.items()}
    if isinstance(value, list):
        return [_resolve_references(item, platform) for item in value]
    if isinstance(value, str):
        return platform.resolve_secret_reference(value)
    return value


def _apply_transformations(df: DataFrame, transform_config: dict[str, Any]) -> DataFrame:
    sql_steps = transform_config.get("sql", [])
    for idx, statement in enumerate(sql_steps):
        view_name = f"_src_{idx}"
        df.createOrReplaceTempView(view_name)
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
    window = Window.partitionBy(*[F.col(key) for key in keys]).orderBy(*[F.expr(expr) for expr in order_by])
    ranked = df.withColumn("__dc_union_rank", F.row_number().over(window))
    return ranked.filter(F.col("__dc_union_rank") == 1).drop("__dc_union_rank")


def _read_dataset_source(
    spark,
    platform: PlatformBase,
    dataset: dict[str, Any],
    *,
    layer: str,
    environment: str,
) -> DataFrame:
    source_conf = dataset["source"]
    if isinstance(source_conf, list):
        dataframes: list[DataFrame] = []
        for idx, source in enumerate(source_conf):
            df = readers.read_batch(
                spark,
                platform,
                source,
                layer=layer,
                dataset=dataset["name"],
                environment=environment,
            )
            df = df.withColumn("__dc_source_ordinal", F.lit(idx))
            dataframes.append(df)
        combined = dataframes[0]
        for frame in dataframes[1:]:
            combined = combined.unionByName(frame, allowMissingColumns=True)
        if dataset.get("merge_strategy"):
            combined = _merge_strategy(combined, dataset["merge_strategy"])
        return combined.drop("__dc_source_ordinal")
    return readers.read_batch(
        spark,
        platform,
        source_conf,
        layer=layer,
        dataset=dataset["name"],
        environment=environment,
    )


def _detect_dataset_issues(dataset: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    incremental_cfg = dataset.get("incremental", {})
    if incremental_cfg.get("mode") == "merge" and not incremental_cfg.get("keys"):
        issues.append("incremental.merge requiere keys definidos")
    if isinstance(dataset.get("source"), list) and not dataset.get("merge_strategy"):
        issues.append("source múltiple requiere merge_strategy para resolver duplicados")
    sink = dataset.get("sink", {})
    if sink.get("type") == "storage" and sink.get("format") not in {"delta", "parquet", "csv", "json", "avro", "orc"}:
        issues.append(f"Formato de sink {sink.get('format')} no soportado para storage")
    return issues


def _build_plan(dataset: dict[str, Any]) -> dict[str, Any]:
    sources = dataset.get("source")
    if isinstance(sources, list):
        source_types = [src.get("type") for src in sources]
    else:
        source_types = [dataset.get("source", {}).get("type")]
    transform_cfg = dataset.get("transform", {})
    plan = {
        "name": dataset["name"],
        "layer": dataset.get("layer"),
        "source_types": source_types,
        "sink": {
            "type": dataset.get("sink", {}).get("type"),
            "format": dataset.get("sink", {}).get("format"),
            "partition_by": dataset.get("sink", {}).get("partition_by"),
        },
        "transform": {
            "sql": transform_cfg.get("sql", []),
            "ops": transform_cfg.get("ops", []),
        },
        "validation": transform_cfg.get("validation", {}),
        "incremental": dataset.get("incremental", {}),
        "streaming": dataset.get("streaming", {}),
    }
    plan["issues"] = _detect_dataset_issues(dataset)
    return plan


def _apply_streaming_options(df: DataFrame, dataset: dict[str, Any]) -> DataFrame:
    streaming_cfg = dataset.get("streaming", {})
    watermark_column = (
        streaming_cfg.get("watermark_column")
        or dataset.get("incremental", {}).get("watermark_column")
    )
    watermark = streaming_cfg.get("watermark")
    if watermark_column and watermark:
        df = df.withWatermark(watermark_column, watermark)
    return df


def _handle_batch_dataset(
    layer: str,
    dataset: dict[str, Any],
    platform: PlatformBase,
    environment: str,
    spark,
) -> dict[str, Any]:
    df = _read_dataset_source(spark, platform, dataset, layer=layer, environment=environment)
    transformed = _apply_transformations(df, dataset.get("transform", {}))
    validation_cfg = dataset.get("transform", {}).get("validation", {})
    validation_result = validation.apply_validation(transformed, validation_cfg)

    if validation_result.metrics.get("invalid_rows", 0) > 0:
        writers.write_rejects(
            validation_result.invalid_df,
            platform,
            dataset["sink"],
            layer=layer,
            dataset=dataset["name"],
            environment=environment,
        )

    metrics = validation_result.metrics
    writers.write_metrics(
        spark,
        platform,
        dataset["sink"],
        layer=layer,
        dataset=dataset["name"],
        environment=environment,
        metrics=metrics,
    )

    incremental_cfg = dataset.get("incremental", {})
    handled = False
    if incremental_cfg.get("mode") == "merge":
        handled = handle_incremental(validation_result.valid_df, dataset["sink"], incremental_cfg)
    if not handled:
        writers.write_batch(validation_result.valid_df, platform, dataset["sink"])
    return {"name": dataset["name"], "status": "completed", "metrics": metrics}


def _handle_streaming_dataset(
    layer: str,
    dataset: dict[str, Any],
    platform: PlatformBase,
    environment: str,
    spark,
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
    checkpoint = dataset.get("streaming", {}).get(
        "checkpoint",
        platform.checkpoint_dir(layer, dataset["name"], environment),
    )
    trigger = dataset.get("streaming", {}).get("trigger")
    writers.write_stream(transformed, platform, dataset["sink"], checkpoint, trigger=trigger)
    return {"name": dataset["name"], "status": "streaming"}


def _process_dataset(
    layer: str,
    dataset: dict[str, Any],
    platform: PlatformBase,
    environment: str,
    spark,
    dry_run: bool = False,
) -> dict[str, Any]:
    dataset_cfg = _resolve_references(dataset, platform)
    LOGGER.info("Procesando dataset %s", dataset_cfg["name"])
    if dry_run:
        plan = _build_plan(dataset_cfg)
        plan["status"] = "planned"
        return plan
    streaming_cfg = dataset_cfg.get("streaming", {})
    if streaming_cfg.get("enabled"):
        return _handle_streaming_dataset(layer, dataset_cfg, platform, environment, spark)
    return _handle_batch_dataset(layer, dataset_cfg, platform, environment, spark)


def run_layer_plan(
    layer: str,
    config: dict[str, Any],
    platform_name: str | None = None,
    environment: str | None = None,
    dry_run: bool = False,
    fail_fast: bool = False,
) -> list[dict[str, Any]]:
    platform = _resolve_platform(platform_name, config)
    spark = _prepare_spark(platform, config)
    results: list[dict[str, Any]] = []
    env = environment or config.get("environment", "dev")
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
                    dry_run=dry_run,
                )
            )
        except Exception as exc:  # pragma: no cover - control de fallos
            LOGGER.error("Fallo en dataset %s: %s", dataset.get("name"), exc)
            if fail_fast:
                raise
            results.append({"name": dataset.get("name"), "status": "failed", "error": str(exc)})
    LOGGER.info("Ejecución de capa %s completada", layer)
    return results
