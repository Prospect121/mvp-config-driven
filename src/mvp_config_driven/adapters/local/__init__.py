"""Local adapters delegating to the legacy runtime components."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml

from ...ports import JobBackendPort, SecretsPort, StoragePort, StreamingPort, TablesPort

try:  # pragma: no cover - optional dependency
    from pyspark.sql import SparkSession  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - pyspark optional
    SparkSession = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import fsspec  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    fsspec = None  # type: ignore

try:  # pragma: no cover - optional dependency
    from datacore.pipeline.db_manager import create_database_manager_from_file
except ImportError:  # pragma: no cover - db manager optional
    create_database_manager_from_file = None  # type: ignore


class LocalStorageAdapter(StoragePort):
    """Storage adapter backed by :mod:`fsspec` when available."""

    def read_text(self, uri: str) -> str:
        if fsspec is None or "://" not in uri or uri.startswith("file://"):
            return Path(uri.replace("file://", "")).read_text(encoding="utf-8")
        with fsspec.open(uri, "r", encoding="utf-8") as handle:  # type: ignore[arg-type]
            return handle.read()

    def exists(self, uri: str) -> bool:
        if fsspec is None or "://" not in uri or uri.startswith("file://"):
            return Path(uri.replace("file://", "")).exists()
        filesystem, path = fsspec.core.url_to_fs(uri)  # type: ignore[attr-defined]
        return bool(filesystem.exists(path))


class LocalTablesAdapter(TablesPort):
    """Adapter that reuses the legacy database manager helpers."""

    def __init__(self, storage: Optional[StoragePort] = None) -> None:
        self._storage = storage or LocalStorageAdapter()

    def load_metadata(self, path: str, environment: str) -> tuple[Any, Dict[str, Any]]:
        payload = yaml.safe_load(self._storage.read_text(path)) or {}
        manager = None
        if create_database_manager_from_file is not None:
            manager = create_database_manager_from_file(path, environment)
        table_settings = dict(payload.get("table_settings") or {})
        return manager, table_settings

    def log_pipeline_execution(
        self, manager: Any, *, dataset_name: str, pipeline_type: str, status: str
    ) -> Optional[str]:
        if manager is None:
            return None
        try:
            return manager.log_pipeline_execution(
                dataset_name=dataset_name,
                pipeline_type=pipeline_type,
                status=status,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"[metadata] Warning: Failed to log pipeline start: {exc}")
            return None


class LocalSecretsAdapter(SecretsPort):
    """Secrets adapter backed by environment variables."""

    def get_secret(self, key: str) -> Optional[str]:
        return os.getenv(key)


class LocalJobBackend(JobBackendPort):
    """Job backend that provisions local Spark sessions when required."""

    def ensure_engine(self, context: Any) -> Any:
        layer_config = getattr(context, "layer_config", None)
        if layer_config is None or getattr(layer_config, "dry_run", False):
            return None
        existing = getattr(context, "engine", None)
        if existing is not None:
            return existing
        if SparkSession is None:  # pragma: no cover - pyspark optional
            raise RuntimeError("pyspark is not installed; cannot create Spark session")
        dataset_cfg = getattr(context, "dataset_cfg", {}) or {}
        env_cfg = getattr(context, "env_cfg", {}) or {}
        app_name = f"cfg-pipeline::{dataset_cfg.get('id', 'unknown')}"
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.session.timeZone", env_cfg.get("timezone", "UTC"))
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
        )
        context.engine = spark
        return spark

    def stop_engine(self, context: Any) -> None:
        engine = getattr(context, "engine", None)
        if engine is None:
            return
        try:
            engine.stop()
        finally:
            context.engine = None


class LocalStreamingAdapter(StreamingPort):
    """No-op streaming adapter used for local testing."""

    def start(self, context: object) -> None:  # pragma: no cover - placeholder
        return None

    def stop(self, context: object) -> None:  # pragma: no cover - placeholder
        return None


def build_default_adapters(config: Mapping[str, object] | None = None) -> dict[str, object]:
    """Return local adapters for every supported port."""

    _ = config  # unused hook for interface parity
    storage = LocalStorageAdapter()
    return {
        "storage": storage,
        "tables": LocalTablesAdapter(storage=storage),
        "secrets": LocalSecretsAdapter(),
        "job_backend": LocalJobBackend(),
        "streaming": LocalStreamingAdapter(),
    }


__all__ = [
    "LocalJobBackend",
    "LocalSecretsAdapter",
    "LocalStorageAdapter",
    "LocalStreamingAdapter",
    "LocalTablesAdapter",
    "build_default_adapters",
]
