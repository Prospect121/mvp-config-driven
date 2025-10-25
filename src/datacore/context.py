from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

try:  # pragma: no cover - optional dependency when running smoke tests
    from pyspark.sql import SparkSession
except ModuleNotFoundError:  # pragma: no cover - allow dry-run environments
    SparkSession = None  # type: ignore

from pipelines.database.db_manager import create_database_manager_from_file

from datacore.config.schema import (
    DatasetConfigModel,
    LayerRuntimeConfigModel,
    migrate_dataset_config,
    migrate_layer_config,
)


@dataclass
class LayerConfig:
    dataset_config: Optional[str] = None
    env_config: Optional[str] = None
    database_config: Optional[str] = None
    environment: str = "default"
    dry_run: bool = False
    layer: str = "raw"
    compute: Dict[str, Any] = field(default_factory=dict)
    io: Dict[str, Any] = field(default_factory=dict)
    transform: Dict[str, Any] = field(default_factory=dict)
    dq: Dict[str, Any] = field(default_factory=dict)
    aliases: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LayerConfig":
        normalized = migrate_layer_config(data or {})
        runtime_model = LayerRuntimeConfigModel.model_validate(normalized)

        source_cfg = runtime_model.io.source or {}
        sink_cfg = runtime_model.io.sink or {}

        dataset = source_cfg.get("dataset_config") or source_cfg.get("dataset")
        env = (
            source_cfg.get("environment_config")
            or source_cfg.get("environment")
            or normalized.get("environment_config")
            or normalized.get("env_config")
        )
        database = sink_cfg.get("database_config") or sink_cfg.get("database")

        return cls(
            dataset_config=str(dataset) if dataset else None,
            env_config=str(env) if env else None,
            database_config=str(database) if database else None,
            environment=str(runtime_model.environment),
            dry_run=bool(runtime_model.dry_run),
            layer=str(runtime_model.layer or "raw"),
            compute=runtime_model.compute.model_dump(by_alias=True),
            io=runtime_model.io.model_dump(by_alias=True),
            transform=dict(runtime_model.transform),
            dq=dict(runtime_model.dq),
            aliases=dict(runtime_model.legacy_aliases),
        )


@dataclass
class PipelineContext:
    layer_config: LayerConfig
    dataset_cfg: Dict[str, Any] = field(default_factory=dict)
    env_cfg: Dict[str, Any] = field(default_factory=dict)
    db_manager: Any = None
    table_settings: Dict[str, Any] = field(default_factory=dict)
    execution_id: Optional[str] = None
    spark: Any = None

    @property
    def dry_run(self) -> bool:
        return self.layer_config.dry_run


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def build_context(raw_config: Dict[str, Any]) -> PipelineContext:
    layer_config = LayerConfig.from_dict(raw_config)

    dataset_cfg: Dict[str, Any] = {}
    env_cfg: Dict[str, Any] = {}
    db_manager = None
    table_settings: Dict[str, Any] = {}
    execution_id: Optional[str] = None

    if not layer_config.dry_run:
        if not layer_config.dataset_config:
            raise ValueError("'dataset_config' is required when dry_run is False")
        if not layer_config.env_config:
            raise ValueError("'environment_config' is required when dry_run is False")

        raw_dataset_cfg = _load_yaml(layer_config.dataset_config)
        migrated_dataset_cfg = migrate_dataset_config(raw_dataset_cfg)
        dataset_model = DatasetConfigModel.model_validate(migrated_dataset_cfg)
        dataset_cfg = dataset_model.model_dump(by_alias=True)
        env_cfg = _load_yaml(layer_config.env_config)

        db_cfg_path = layer_config.database_config
        if db_cfg_path and os.path.exists(db_cfg_path):
            full_db_cfg = _load_yaml(db_cfg_path)
            db_manager = create_database_manager_from_file(db_cfg_path, layer_config.environment)
            table_settings = full_db_cfg.get("table_settings", {})
            try:
                execution_id = db_manager.log_pipeline_execution(
                    dataset_name=dataset_cfg.get("id", "unknown"),
                    pipeline_type="etl",
                    status="started",
                )
                print(f"[metadata] Pipeline execution started: {execution_id}")
            except Exception as exc:  # pragma: no cover - defensive logging
                print(f"[metadata] Warning: Failed to log pipeline start: {exc}")
        elif db_cfg_path:
            print(f"[gold] Database config file not found: {db_cfg_path}. Skipping Gold layer.")

    return PipelineContext(
        layer_config=layer_config,
        dataset_cfg=dataset_cfg,
        env_cfg=env_cfg,
        db_manager=db_manager,
        table_settings=table_settings,
        execution_id=execution_id,
    )


def ensure_spark(context: PipelineContext) -> Any:
    if context.dry_run:
        return None

    if context.spark is not None:
        return context.spark

    if SparkSession is None:  # pragma: no cover - pyspark unavailable in smoke tests
        raise RuntimeError("pyspark is not installed; cannot create Spark session")

    cfg = context.dataset_cfg
    env = context.env_cfg
    app_name = f"cfg-pipeline::{cfg.get('id', 'unknown')}"
    context.spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", env.get("timezone", "UTC"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    return context.spark


def stop_spark(context: PipelineContext) -> None:
    spark = context.spark
    if spark is None:
        return
    try:
        spark.stop()
    finally:
        context.spark = None


def context_paths(context: PipelineContext) -> Dict[str, Path]:
    cfg = context.layer_config
    return {
        "dataset": Path(cfg.dataset_config) if cfg.dataset_config else None,  # type: ignore[dict-item]
        "environment": Path(cfg.env_config) if cfg.env_config else None,  # type: ignore[dict-item]
        "database": Path(cfg.database_config) if cfg.database_config else None,  # type: ignore[dict-item]
    }
