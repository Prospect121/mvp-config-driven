"""Runtime layer configuration models used by the orchestration core."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from datacore.config.schema import (
    DatasetConfigModel,
    LayerRuntimeConfigModel,
    migrate_dataset_config,
    migrate_layer_config,
)


@dataclass
class LayerConfig:
    """Normalized runtime configuration for a single pipeline layer."""

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
    storage: Dict[str, Any] = field(default_factory=dict)
    aliases: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LayerConfig":
        """Build a :class:`LayerConfig` from a raw mapping."""

        normalized = migrate_layer_config(data or {})
        runtime_model = LayerRuntimeConfigModel.model_validate(normalized)

        io_dump = runtime_model.io.model_dump(by_alias=True)
        source_cfg_raw = io_dump.get("source") or {}
        sink_cfg_raw = io_dump.get("sink") or {}

        if isinstance(source_cfg_raw, list):
            source_cfg = source_cfg_raw[0] if source_cfg_raw else {}
        else:
            source_cfg = source_cfg_raw
        if isinstance(sink_cfg_raw, list):
            sink_cfg = sink_cfg_raw[0] if sink_cfg_raw else {}
        else:
            sink_cfg = sink_cfg_raw

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
            io=io_dump,
            transform=runtime_model.transform.model_dump(by_alias=True),
            dq=runtime_model.dq.model_dump(by_alias=True),
            storage=dict(runtime_model.storage),
            aliases=dict(runtime_model.legacy_aliases),
        )


__all__ = ["LayerConfig", "DatasetConfigModel", "migrate_dataset_config"]
