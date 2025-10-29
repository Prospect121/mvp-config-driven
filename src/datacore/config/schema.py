from __future__ import annotations

from copy import deepcopy
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _merge_dicts(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


class FlexibleModel(BaseModel):
    """Base class that allows unknown keys while preserving them when dumped."""

    model_config = ConfigDict(extra="allow")


class SparkConfig(FlexibleModel):
    app_name: Optional[str] = Field(default=None, description="Spark application name")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Spark configuration overrides"
    )


class ComputeConfig(FlexibleModel):
    engine: str = Field(default="spark", description="Execution engine identifier")
    spark: SparkConfig = Field(
        default_factory=SparkConfig,
        description="Spark specific execution configuration",
    )


class WatermarkConfig(FlexibleModel):
    field: Optional[str] = Field(default=None, description="Column used as watermark")
    state_id: Optional[str] = Field(
        default=None, description="Persistent identifier used to store the watermark"
    )
    default: Optional[str] = Field(
        default="1970-01-01T00:00:00Z",
        description="Fallback value applied when no persisted state exists",
    )
    param: Optional[str] = Field(
        default=None,
        description="Optional HTTP parameter name used when applying the watermark",
    )
    placeholder: Optional[str] = Field(
        default=None,
        description="Optional placeholder replaced within SQL queries",
    )
    from_env: Optional[str] = Field(
        default=None, description="Environment variable used as fallback value"
    )


class IncrementalConfig(FlexibleModel):
    watermark: Optional[WatermarkConfig] = Field(
        default=None, description="Watermark definition for incremental ingestion"
    )


class PaginationConfig(FlexibleModel):
    strategy: str = Field(
        default="none",
        description="Pagination strategy (none|param_increment|cursor|link_header)",
    )
    param: Optional[str] = Field(
        default=None, description="Query parameter used for param_increment pagination"
    )
    cursor_param: Optional[str] = Field(
        default=None, description="Cursor parameter used for cursor pagination"
    )
    cursor_field: Optional[str] = Field(
        default=None, description="Payload field containing the next cursor"
    )
    max_pages: Optional[int] = Field(
        default=None, description="Maximum number of pages to request"
    )
    stop_on_empty: Optional[bool] = Field(
        default=True, description="Whether to stop when an empty page is encountered"
    )


class RateLimitConfig(FlexibleModel):
    requests_per_minute: Optional[float] = Field(
        default=None, description="Maximum number of requests per minute"
    )


class RetryConfig(FlexibleModel):
    max_attempts: Optional[int] = Field(default=3, description="Maximum retry attempts")
    backoff: Optional[float] = Field(default=1.5, description="Backoff factor")
    jitter: Optional[float] = Field(default=0.25, description="Jitter factor")
    status_forcelist: Optional[List[int]] = Field(
        default=None, description="HTTP status codes that should trigger retries"
    )


class HTTPAuthConfig(FlexibleModel):
    type: str = Field(default="none", description="Authentication strategy identifier")
    env: Optional[str] = Field(
        default=None, description="Environment variable used by bearer/api-key auth"
    )
    header: Optional[str] = Field(
        default=None, description="Header name for api_key_header authentication"
    )
    username_env: Optional[str] = Field(
        default=None, description="Environment variable holding the username"
    )
    password_env: Optional[str] = Field(
        default=None, description="Environment variable holding the password"
    )
    token_url: Optional[str] = Field(
        default=None, description="Token endpoint for OAuth2 client credentials"
    )
    client_id_env: Optional[str] = Field(
        default=None, description="Environment variable with the OAuth client id"
    )
    client_secret_env: Optional[str] = Field(
        default=None, description="Environment variable with the OAuth client secret"
    )
    scope: Optional[str] = Field(
        default=None, description="Scope requested during OAuth token acquisition"
    )


class HTTPSchema(FlexibleModel):
    type: Literal["http"] = "http"
    url: str = Field(description="Endpoint URL")
    method: str = Field(default="GET", description="HTTP verb to use")
    headers: Dict[str, Any] = Field(
        default_factory=dict, description="Static HTTP headers to include"
    )
    params: Dict[str, Any] = Field(
        default_factory=dict, description="Static query parameters"
    )
    pagination: PaginationConfig = Field(
        default_factory=PaginationConfig, description="Pagination settings"
    )
    rate_limit: RateLimitConfig = Field(
        default_factory=RateLimitConfig, description="Rate limiting configuration"
    )
    retries: RetryConfig = Field(
        default_factory=RetryConfig, description="Retry configuration"
    )
    timeout: Optional[float] = Field(
        default=30, description="Request timeout in seconds"
    )
    auth: HTTPAuthConfig = Field(
        default_factory=HTTPAuthConfig, description="Authentication configuration"
    )
    incremental: IncrementalConfig = Field(
        default_factory=IncrementalConfig, description="Incremental ingestion hints"
    )


class JDBCAuthConfig(FlexibleModel):
    mode: Optional[str] = Field(
        default=None, description="Authentication mode (managed_identity|basic_env)"
    )
    user_env: Optional[str] = Field(
        default=None, description="Environment variable containing the username"
    )
    username_env: Optional[str] = Field(
        default=None, description="Alternate username environment variable"
    )
    password_env: Optional[str] = Field(
        default=None, description="Environment variable containing the password"
    )
    use_managed_identity: Optional[bool] = Field(
        default=None, description="Whether to use a managed identity"
    )
    user: Optional[str] = Field(default=None, description="Explicit username override")
    username: Optional[str] = Field(
        default=None, description="Explicit username override"
    )
    password: Optional[str] = Field(default=None, description="Explicit password")


class JDBCPartitioningConfig(FlexibleModel):
    column: Optional[str] = Field(
        default=None, description="Column used to partition the JDBC load"
    )
    num_partitions: Optional[int] = Field(
        default=None, description="Number of partitions to create"
    )
    lower_bound: Optional[Any] = Field(
        default=None, description="Lower bound used for partitioning"
    )
    upper_bound: Optional[Any] = Field(
        default=None, description="Upper bound used for partitioning"
    )
    discover: Optional[bool] = Field(
        default=True,
        description="Whether bounds should be auto-discovered when not provided",
    )
    alias: Optional[str] = Field(
        default=None,
        description="Optional alias used when computing bounds via sub-queries",
    )


class JDBCSchema(FlexibleModel):
    type: Literal["jdbc"] = "jdbc"
    url: str = Field(description="JDBC URL")
    driver: str = Field(description="JDBC driver class")
    query: Optional[str] = Field(default=None, description="Query to execute")
    table: Optional[str] = Field(default=None, description="Table to select from")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional Spark reader options"
    )
    auth: JDBCAuthConfig = Field(
        default_factory=JDBCAuthConfig, description="Authentication configuration"
    )
    incremental: IncrementalConfig = Field(
        default_factory=IncrementalConfig, description="Incremental ingestion hints"
    )
    partitioning: JDBCPartitioningConfig = Field(
        default_factory=JDBCPartitioningConfig,
        description="Partitioning configuration",
    )


class FileIOSchema(FlexibleModel):
    type: Literal["files"] = "files"
    uri: Optional[str] = Field(default=None, description="Fully qualified URI")
    path: Optional[str] = Field(default=None, description="Filesystem path")
    format: Optional[str] = Field(default=None, description="Dataset format")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Reader/Writer specific options"
    )
    filesystem: Dict[str, Any] = Field(
        default_factory=dict, description="Filesystem adapter overrides"
    )
    mode: Optional[str] = Field(default=None, description="Write mode when used as sink")
    partition_by: Optional[List[str]] = Field(
        default=None, description="Partitioning columns when writing"
    )
    coalesce: Optional[int] = Field(
        default=None, description="Number of partitions to coalesce to when writing"
    )
    repartition: Optional[int] = Field(
        default=None, description="Number of partitions to repartition to when writing"
    )


LayerIOSpec = Annotated[
    Union[HTTPSchema, JDBCSchema, FileIOSchema],
    Field(discriminator="type"),
]


class IOConfig(FlexibleModel):
    source: Optional[Union[LayerIOSpec, Dict[str, Any], List[Union[LayerIOSpec, Dict[str, Any]]]]] = Field(
        default=None, description="Source declaration for the layer"
    )
    sink: Optional[Union[LayerIOSpec, Dict[str, Any], List[Union[LayerIOSpec, Dict[str, Any]]]]] = Field(
        default=None, description="Sink declaration for the layer"
    )


class TransformConfig(FlexibleModel):
    pre: List[Dict[str, Any]] = Field(
        default_factory=list, description="List of pre-processing transformations"
    )
    sql: Optional[Union[str, List[str]]] = Field(
        default=None, description="SQL statements applied during the transform stage"
    )
    functions: List[Any] = Field(
        default_factory=list, description="Callable transformations"
    )
    references: Dict[str, Any] = Field(
        default_factory=dict, description="External references (SQL, schemas, etc.)"
    )


class DQReportConfig(FlexibleModel):
    path: Optional[str] = Field(
        default=None, description="Base path used to persist DQ reports"
    )
    base_path: Optional[str] = Field(
        default=None, description="Legacy alias for 'path'"
    )


class DQConfig(FlexibleModel):
    expectations: Union[List[Dict[str, Any]], Dict[str, Any]] = Field(
        default_factory=list, description="Quality expectations"
    )
    fail_on_error: bool = Field(
        default=True, description="Whether expectation failures should abort execution"
    )
    report: Optional[DQReportConfig] = Field(
        default=None, description="Report persistence configuration"
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
    source: Optional[Union[LayerIOSpec, Dict[str, Any], List[Union[LayerIOSpec, Dict[str, Any]]]]] = Field(default=None)
    sink: Optional[Union[LayerIOSpec, Dict[str, Any], List[Union[LayerIOSpec, Dict[str, Any]]]]] = Field(default=None)


class LayerRuntimeConfigModel(FlexibleModel):
    layer: Optional[str] = Field(default=None, description="Layer name (raw, bronze, ...)")
    dry_run: bool = Field(default=False, description="Whether to skip real execution")
    environment: str = Field(default="default", description="Target environment identifier")
    compute: ComputeConfig = Field(default_factory=ComputeConfig)
    io: LayerRuntimeIO = Field(default_factory=LayerRuntimeIO)
    transform: TransformConfig = Field(default_factory=TransformConfig)
    dq: DQConfig = Field(default_factory=DQConfig)
    storage: Dict[str, Any] = Field(default_factory=dict)
    extends: Optional[str] = Field(
        default=None, description="Optional base configuration used for inheritance"
    )
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
    ) and "transformations" not in cfg:
        cfg.setdefault("_legacy_aliases", {})
        return cfg

    if any(key in cfg for key in ("transformations", "quality", "platform", "storage", "incremental")):
        platform_block = cfg.get("platform")
        if isinstance(platform_block, str):
            platform_block = {"name": platform_block}
        elif not isinstance(platform_block, dict):
            platform_block = {}

        compute_block = platform_block.get("compute")
        if isinstance(compute_block, dict):
            existing_compute = cfg.get("compute")
            if isinstance(existing_compute, dict):
                cfg["compute"] = _merge_dicts(compute_block, existing_compute)
            else:
                cfg["compute"] = compute_block

        storage_defaults = platform_block.get("storage")
        storage_block = cfg.get("storage")
        if isinstance(storage_defaults, dict):
            if isinstance(storage_block, dict):
                storage_block = _merge_dicts(storage_defaults, storage_block)
            else:
                storage_block = storage_defaults
            cfg["storage"] = storage_block
        elif not isinstance(storage_block, dict):
            storage_block = {}
            cfg["storage"] = storage_block

        io_block: Dict[str, Any] = {}
        if isinstance(storage_block, dict):
            dataset_path = storage_block.get("dataset_config") or storage_block.get("dataset")
            env_path = (
                storage_block.get("environment_config")
                or storage_block.get("environment")
                or storage_block.get("env_config")
            )
            database_path = storage_block.get("database_config") or storage_block.get("database")

            if dataset_path:
                cfg["dataset_config"] = dataset_path
            if env_path:
                cfg["environment_config"] = env_path
                cfg["env_config"] = env_path
            if database_path:
                cfg["database_config"] = database_path

            source_from_storage = storage_block.get("source")
            sink_from_storage = storage_block.get("sink")

            if isinstance(source_from_storage, dict):
                io_block["source"] = deepcopy(source_from_storage)
            if isinstance(sink_from_storage, dict):
                io_block.setdefault("sink", {})
                io_block["sink"] = deepcopy(sink_from_storage)

        if isinstance(cfg.get("io"), dict):
            io_block = _merge_dicts(io_block, cfg["io"])
        if io_block:
            cfg["io"] = io_block

        transformations = cfg.pop("transformations", None)
        if isinstance(transformations, dict):
            existing_transform = cfg.get("transform")
            merged_transform = (
                _merge_dicts(transformations, existing_transform)
                if isinstance(existing_transform, dict)
                else transformations
            )
            cfg["transform"] = merged_transform

        quality_block = cfg.pop("quality", None)
        if isinstance(quality_block, dict):
            existing_dq = cfg.get("dq")
            merged_dq = (
                _merge_dicts(quality_block, existing_dq)
                if isinstance(existing_dq, dict)
                else quality_block
            )
            cfg["dq"] = merged_dq

        incremental_block = cfg.pop("incremental", None)
        if isinstance(incremental_block, dict) and incremental_block:
            io_section = cfg.setdefault("io", {})
            source_section = io_section.setdefault("source", {})
            existing_incremental = source_section.get("incremental")
            if isinstance(existing_incremental, dict):
                source_section["incremental"] = _merge_dicts(
                    incremental_block, existing_incremental
                )
            else:
                source_section["incremental"] = incremental_block

        cfg.setdefault("_legacy_aliases", {})

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
    "SparkConfig",
    "ComputeConfig",
    "WatermarkConfig",
    "IncrementalConfig",
    "PaginationConfig",
    "RateLimitConfig",
    "RetryConfig",
    "HTTPAuthConfig",
    "HTTPSchema",
    "JDBCAuthConfig",
    "JDBCPartitioningConfig",
    "JDBCSchema",
    "FileIOSchema",
    "IOConfig",
    "TransformConfig",
    "DQConfig",
    "DQReportConfig",
    "LayerBlock",
    "LayerRuntimeConfigModel",
    "LayerRuntimeIO",
    "DatasetConfigModel",
    "migrate_layer_config",
    "migrate_dataset_config",
    "layer_runtime_json_schema",
    "dataset_json_schema",
]

