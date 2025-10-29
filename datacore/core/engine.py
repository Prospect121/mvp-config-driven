"""Motor de ejecución por capas."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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


def _prepare_spark(platform: PlatformBase, config: dict[str, Any]) -> Any:
    spark_conf: dict[str, Any] = {}
    spark_section = config.get("spark", {})
    if "shuffle_partitions" in spark_section:
        spark_conf["spark.sql.shuffle.partitions"] = spark_section["shuffle_partitions"]
    if "extra_conf" in spark_section:
        spark_conf.update(spark_section["extra_conf"])
    return platform.build_spark_session(spark_conf)


def _apply_transformations(df: DataFrame, transform_config: dict[str, Any]) -> DataFrame:
    sql_steps = transform_config.get("sql", [])
    for statement in sql_steps:
        df.createOrReplaceTempView("_src")
        df = df.sparkSession.sql(statement)
    registered = transform_config.get("udf", [])
    if registered:
        df = transforms.apply_registered(df, registered)
    if transform_config.get("add_ingestion_ts", True):
        df = df.withColumn("_ingestion_ts", F.current_timestamp())
    return df


def _process_dataset(
    layer: str,
    dataset: dict[str, Any],
    platform: PlatformBase,
    environment: str,
    spark_session,
    dry_run: bool = False,
) -> dict[str, Any]:
    LOGGER.info("Procesando dataset %s", dataset["name"])
    if dry_run:
        return {"name": dataset["name"], "status": "planned"}
    streaming_cfg = dataset.get("streaming", {"enabled": False})
    if streaming_cfg.get("enabled"):
        df_stream = readers.read_stream(spark_session, platform, dataset["source"])
        transformed = _apply_transformations(df_stream, dataset.get("transform", {}))
        checkpoint = platform.checkpoint_dir(layer, dataset["name"], environment)
        writers.write_stream(transformed, platform, dataset["sink"], checkpoint)
        return {"name": dataset["name"], "status": "streaming"}
    df = readers.read_batch(spark_session, platform, dataset["source"])
    transformed = _apply_transformations(df, dataset.get("transform", {}))
    metrics = validation.apply_validation(
        transformed,
        dataset.get("transform", {}).get("validation", {}),
    )
    incremental_cfg = dataset.get("incremental", {"mode": "append"})
    mode = incremental_cfg.get("mode", "append")
    if mode == "merge" and incremental_cfg.get("keys"):
        handle_incremental(transformed, dataset["sink"], mode, incremental_cfg["keys"])
    else:
        writers.write_batch(transformed, platform, dataset["sink"])
    return {"name": dataset["name"], "status": "completed", "metrics": metrics}


def run_layer_plan(
    layer: str,
    config: dict[str, Any],
    platform_name: str | None = None,
    environment: str | None = None,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    platform = _resolve_platform(platform_name, config)
    spark = _prepare_spark(platform, config)
    results: list[dict[str, Any]] = []
    env = environment or config.get("environment", "dev")
    for dataset in config.get("datasets", []):
        if dataset.get("layer") != layer:
            continue
        results.append(_process_dataset(layer, dataset, platform, env, spark, dry_run=dry_run))
    LOGGER.info("Ejecución de capa %s completada", layer)
    return results
