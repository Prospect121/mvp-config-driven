"""Pipeline context helpers built on top of the orchestration contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from ..adapters.local import build_default_adapters
from ..ports import JobBackendPort, SecretsPort, StoragePort, StreamingPort, TablesPort
from .config import DatasetConfigModel, LayerConfig, migrate_dataset_config


@dataclass
class PipelineContext:
    """Aggregated execution state passed across orchestration helpers."""

    layer_config: LayerConfig
    dataset_cfg: Dict[str, Any] = field(default_factory=dict)
    env_cfg: Dict[str, Any] = field(default_factory=dict)
    table_settings: Dict[str, Any] = field(default_factory=dict)
    execution_id: Optional[str] = None
    engine: Any = None
    storage: Optional[StoragePort] = None
    tables: Optional[TablesPort] = None
    secrets: Optional[SecretsPort] = None
    job_backend: Optional[JobBackendPort] = None
    streaming: Optional[StreamingPort] = None

    @property
    def dry_run(self) -> bool:
        return self.layer_config.dry_run

    def ensure_engine(self) -> Any:
        if self.job_backend is None:
            return None
        self.engine = self.job_backend.ensure_engine(self)
        return self.engine

    def stop_engine(self) -> None:
        if self.job_backend is None:
            return
        self.job_backend.stop_engine(self)
        self.engine = None


def _load_yaml(storage: StoragePort, path: str) -> Dict[str, Any]:
    payload = storage.read_text(path)
    return yaml.safe_load(payload) or {}


def build_context(
    raw_config: Dict[str, Any],
    *,
    storage: StoragePort | None = None,
    tables: TablesPort | None = None,
    secrets: SecretsPort | None = None,
    job_backend: JobBackendPort | None = None,
    streaming: StreamingPort | None = None,
) -> PipelineContext:
    """Construct a :class:`PipelineContext` from a raw layer configuration."""

    defaults = build_default_adapters()
    storage_port = storage or defaults["storage"]  # type: ignore[assignment]
    tables_port = tables or defaults["tables"]  # type: ignore[assignment]
    secrets_port = secrets or defaults["secrets"]  # type: ignore[assignment]
    job_backend_port = job_backend or defaults["job_backend"]  # type: ignore[assignment]
    streaming_port = streaming or defaults["streaming"]  # type: ignore[assignment]

    layer_config = LayerConfig.from_dict(raw_config)

    dataset_cfg: Dict[str, Any] = {}
    env_cfg: Dict[str, Any] = {}
    table_settings: Dict[str, Any] = {}
    execution_id: Optional[str] = None

    if not layer_config.dry_run:
        if not layer_config.dataset_config:
            raise ValueError("'dataset_config' is required when dry_run is False")
        if not layer_config.env_config:
            raise ValueError("'environment_config' is required when dry_run is False")

        dataset_path = layer_config.dataset_config
        env_path = layer_config.env_config

        if not storage_port.exists(dataset_path):
            raise FileNotFoundError(f"Dataset configuration not found: {dataset_path}")
        if not storage_port.exists(env_path):
            raise FileNotFoundError(f"Environment configuration not found: {env_path}")

        raw_dataset_cfg = _load_yaml(storage_port, dataset_path)
        migrated_dataset_cfg = migrate_dataset_config(raw_dataset_cfg)
        dataset_model = DatasetConfigModel.model_validate(migrated_dataset_cfg)
        dataset_cfg = dataset_model.model_dump(by_alias=True)
        env_cfg = _load_yaml(storage_port, env_path)

        db_cfg_path = layer_config.database_config
        if db_cfg_path and storage_port.exists(db_cfg_path):
            manager, table_settings = tables_port.load_metadata(db_cfg_path, layer_config.environment)
            dataset_name = dataset_cfg.get("id", "unknown")
            execution_id = tables_port.log_pipeline_execution(
                manager,
                dataset_name=str(dataset_name),
                pipeline_type="etl",
                status="started",
            )
        elif db_cfg_path:
            print(f"[gold] Database config file not found: {db_cfg_path}. Skipping Gold layer.")

    return PipelineContext(
        layer_config=layer_config,
        dataset_cfg=dataset_cfg,
        env_cfg=env_cfg,
        table_settings=table_settings,
        execution_id=execution_id,
        storage=storage_port,
        tables=tables_port,
        secrets=secrets_port,
        job_backend=job_backend_port,
        streaming=streaming_port,
    )


def ensure_engine(context: PipelineContext) -> Any:
    """Ensure the execution engine is provisioned using the configured job backend."""

    return context.ensure_engine()


def stop_engine(context: PipelineContext) -> None:
    """Stop the execution engine via the configured job backend."""

    context.stop_engine()


def context_paths(context: PipelineContext) -> Dict[str, Path]:
    """Return a mapping with the resolved configuration paths."""

    cfg = context.layer_config
    return {
        "dataset": Path(cfg.dataset_config) if cfg.dataset_config else None,  # type: ignore[dict-item]
        "environment": Path(cfg.env_config) if cfg.env_config else None,  # type: ignore[dict-item]
        "database": Path(cfg.database_config) if cfg.database_config else None,  # type: ignore[dict-item]
    }


__all__ = [
    "PipelineContext",
    "build_context",
    "context_paths",
    "ensure_engine",
    "stop_engine",
]
