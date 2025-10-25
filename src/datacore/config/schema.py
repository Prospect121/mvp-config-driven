from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class FlexibleModel(BaseModel):
    """Base class that allows unknown keys while preserving them when dumped."""

    model_config = ConfigDict(extra="allow")


class ComputeConfig(FlexibleModel):
    engine: str = Field(default="spark", description="Execution engine identifier")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Engine specific tuning options"
    )


class IOConfig(FlexibleModel):
    source: Optional[Dict[str, Any] | List[Dict[str, Any]]] = Field(
        default_factory=dict, description="Source declaration for the layer"
    )
    sink: Optional[Dict[str, Any] | List[Dict[str, Any]]] = Field(
        default_factory=dict, description="Sink declaration for the layer"
    )


class TransformConfig(FlexibleModel):
    steps: List[Dict[str, Any]] = Field(
        default_factory=list, description="Declarative ordered list of transformations"
    )
    standardization: Dict[str, Any] = Field(
        default_factory=dict,
        description="Legacy standardization block kept as part of the transform stage",
    )
    references: Dict[str, Any] = Field(
        default_factory=dict, description="External references (SQL, schemas, etc.)"
    )


class DQConfig(FlexibleModel):
    expectations: Dict[str, Any] = Field(
        default_factory=dict,
        description="Quality expectations and quarantine configuration",
    )


class LayerBlock(FlexibleModel):
    compute: ComputeConfig = Field(default_factory=ComputeConfig)
    io: IOConfig = Field(default_factory=IOConfig)
    transform: TransformConfig = Field(default_factory=TransformConfig)
    dq: DQConfig = Field(default_factory=DQConfig)


class DatasetConfigModel(FlexibleModel):
    id: Optional[str] = Field(default=None, description="Dataset identifier")
    description: Optional[str] = Field(default=None, description="Dataset description")
    layers: Dict[str, LayerBlock] = Field(
        default_factory=dict,
        description="Per-layer configuration expressed with compute/io/transform/dq",
    )
    legacy_aliases: Dict[str, str] = Field(
        default_factory=dict,
        alias="_legacy_aliases",
        description="Mapping of legacy keys to their new location",
    )


class LayerRuntimeIO(FlexibleModel):
    source: Dict[str, Any] = Field(default_factory=dict)
    sink: Dict[str, Any] = Field(default_factory=dict)


class LayerRuntimeConfigModel(FlexibleModel):
    layer: Optional[str] = Field(default=None, description="Layer name (raw, bronze, ...)")
    dry_run: bool = Field(default=False, description="Whether to skip real execution")
    environment: str = Field(default="default", description="Target environment identifier")
    compute: ComputeConfig = Field(default_factory=ComputeConfig)
    io: LayerRuntimeIO = Field(default_factory=LayerRuntimeIO)
    transform: Dict[str, Any] = Field(default_factory=dict)
    dq: Dict[str, Any] = Field(default_factory=dict)
    storage: Dict[str, Any] = Field(default_factory=dict)
    legacy_aliases: Dict[str, str] = Field(
        default_factory=dict,
        alias="_legacy_aliases",
        description="Mapping of legacy keys to their new location",
    )


def _ensure_layer_block(layers: Dict[str, Dict[str, Any]], name: str) -> Dict[str, Any]:
    block = layers.get(name)
    if not isinstance(block, dict):
        block = {}
    block.setdefault("compute", {"engine": "spark"})
    block.setdefault("io", {})
    block.setdefault("transform", {})
    block.setdefault("dq", {})
    layers[name] = block
    return block


def migrate_dataset_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Translate legacy dataset configurations into the layered schema."""

    cfg: Dict[str, Any] = deepcopy(raw or {})
    if not isinstance(cfg, dict):
        raise TypeError("Dataset configuration must be a mapping")

    if isinstance(cfg.get("layers"), dict):
        cfg.setdefault("_legacy_aliases", {})
        return cfg

    layers: Dict[str, Dict[str, Any]] = {}
    aliases: Dict[str, str] = {}

    # Raw layer: ingestion definition
    if "sources" in cfg or "source" in cfg:
        raw_layer = _ensure_layer_block(layers, "raw")
        raw_layer.setdefault("io", {})["source"] = cfg.get("sources") or cfg.get("source")
        aliases["source"] = "layers.raw.io.source"
        aliases["sources"] = "layers.raw.io.source"

    # Output sections define sinks for subsequent layers
    outputs = cfg.get("output")
    if isinstance(outputs, dict):
        for layer_name, sink in outputs.items():
            layer_block = _ensure_layer_block(layers, layer_name)
            layer_block.setdefault("io", {})["sink"] = sink
            aliases[f"output.{layer_name}"] = f"layers.{layer_name}.io.sink"

    # Transform related keys belong to the Silver layer by default
    transform_keys = [
        "standardization",
        "select_columns",
        "schema",
        "transforms_ref",
        "json_normalization",
        "null_handling",
    ]
    if any(key in cfg for key in transform_keys):
        silver_block = _ensure_layer_block(layers, "silver")
        transform_section = silver_block.setdefault("transform", {})
        for key in transform_keys:
            if key in cfg:
                transform_section[key] = cfg[key]
                aliases[key] = f"layers.silver.transform.{key}"

    # Quality configuration maps to the dq section
    if "quality" in cfg:
        silver_block = _ensure_layer_block(layers, "silver")
        dq_section = silver_block.setdefault("dq", {})
        dq_section["expectations"] = cfg["quality"]
        aliases["quality"] = "layers.silver.dq.expectations"

    # Ensure we always expose the layers key
    cfg["layers"] = layers
    cfg["_legacy_aliases"] = aliases
    return cfg


def migrate_layer_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Translate legacy layer run configuration into the runtime schema."""

    cfg: Dict[str, Any] = deepcopy(raw or {})
    if not isinstance(cfg, dict):
        raise TypeError("Layer configuration must be a mapping")

    if isinstance(cfg.get("io"), dict) and (
        "source" in cfg["io"] or "sink" in cfg["io"]
    ):
        cfg.setdefault("_legacy_aliases", {})
        return cfg

    paths = cfg.pop("paths", {}) or {}
    dataset_path = cfg.pop("dataset_config", None) or paths.get("dataset")
    env_path = (
        cfg.pop("environment_config", None)
        or cfg.pop("env_config", None)
        or paths.get("environment")
    )
    database_path = cfg.pop("database_config", None) or paths.get("database")

    io_block: Dict[str, Any] = {}
    aliases: Dict[str, str] = {}

    source_block: Dict[str, Any] = {}
    if dataset_path:
        source_block["dataset_config"] = dataset_path
        aliases["dataset_config"] = "io.source.dataset_config"
    if env_path:
        source_block["environment_config"] = env_path
        aliases["environment_config"] = "io.source.environment_config"
    if source_block:
        io_block["source"] = source_block

    sink_block: Dict[str, Any] = {}
    if database_path:
        sink_block["database_config"] = database_path
        aliases["database_config"] = "io.sink.database_config"
    if sink_block:
        io_block["sink"] = sink_block

    if io_block:
        cfg["io"] = io_block

    cfg.setdefault("compute", {"engine": "spark"})
    cfg.setdefault("transform", {})
    cfg.setdefault("dq", {})
    cfg["_legacy_aliases"] = aliases

    # Preserve direct references for backwards compatibility with callers that
    # still expect flat keys. They point to the same data used in the new schema.
    if dataset_path is not None:
        cfg["dataset_config"] = dataset_path
    if env_path is not None:
        cfg["environment_config"] = env_path
        cfg["env_config"] = env_path
    if database_path is not None:
        cfg["database_config"] = database_path

    return cfg


def layer_runtime_json_schema() -> Dict[str, Any]:
    return LayerRuntimeConfigModel.model_json_schema()


def dataset_json_schema() -> Dict[str, Any]:
    return DatasetConfigModel.model_json_schema()


__all__ = [
    "ComputeConfig",
    "IOConfig",
    "TransformConfig",
    "DQConfig",
    "LayerBlock",
    "LayerRuntimeConfigModel",
    "LayerRuntimeIO",
    "DatasetConfigModel",
    "migrate_layer_config",
    "migrate_dataset_config",
    "layer_runtime_json_schema",
    "dataset_json_schema",
]

