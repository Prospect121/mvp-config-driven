"""Compatibility layer exposing the refactored orchestration context."""

from mvp_config_driven.core.context import (  # noqa: F401
    PipelineContext,
    build_context,
    context_paths,
    ensure_engine,
    stop_engine,
)
from mvp_config_driven.core.config import LayerConfig  # noqa: F401

ensure_spark = ensure_engine  # Backwards compatibility alias
stop_spark = stop_engine  # Backwards compatibility alias

__all__ = [
    "LayerConfig",
    "PipelineContext",
    "build_context",
    "context_paths",
    "ensure_engine",
    "ensure_spark",
    "stop_engine",
    "stop_spark",
]
