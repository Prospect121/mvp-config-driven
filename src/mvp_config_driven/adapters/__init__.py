"""Adapter factories bridging orchestration ports with concrete backends."""

from __future__ import annotations

import os
from typing import Callable, Dict, Mapping

AdapterMap = dict[str, object]
AdapterFactory = Callable[[Mapping[str, object]], AdapterMap]

_FACTORIES: Dict[str, AdapterFactory] = {}
_INITIALIZED = False


def register_adapter_factory(name: str, factory: AdapterFactory) -> None:
    """Register ``factory`` under ``name`` for later discovery."""

    normalized = name.strip().lower()
    _FACTORIES[normalized] = factory


def _ensure_defaults() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return

    from .local import build_default_adapters as _build_local

    register_adapter_factory("local", _build_local)

    try:  # Optional dependency guard - AWS adapters require boto3/s3fs
        from .aws import build_default_adapters as _build_aws
    except ImportError:  # pragma: no cover - boto3 optional in some deployments
        pass
    else:
        register_adapter_factory("aws", _build_aws)

    _INITIALIZED = True


def build_adapters(
    *, platform: str | None = None, config: Mapping[str, object] | None = None
) -> AdapterMap:
    """Instantiate adapters for ``platform`` using an optional ``config``."""

    _ensure_defaults()
    resolved = (platform or os.getenv("PRODI_PLATFORM") or "local").strip().lower()
    factory = _FACTORIES.get(resolved)
    if factory is None:
        available = ", ".join(sorted(_FACTORIES)) or "none"
        raise ValueError(
            f"No adapter factory registered for platform '{resolved}'. Available: {available}"
        )
    return factory(config or {})


__all__ = ["AdapterFactory", "AdapterMap", "build_adapters", "register_adapter_factory"]
