"""Protocol-aware adapters for filesystem-backed IO.

This module centralizes how URIs are normalized and how storage options,
reader options and writer options are derived from the environment
configuration. It builds on top of :mod:`datacore.io.fs` utilities in order to
provide a single place that understands multi-cloud settings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from .fs import storage_options_from_env, _split_uri


def _deep_merge(base: Dict[str, Any], extra: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not extra:
        return dict(base)

    result: Dict[str, Any] = dict(base)
    for key, value in extra.items():
        if (
            isinstance(value, Mapping)
            and key in result
            and isinstance(result[key], Mapping)
        ):
            result[key] = _deep_merge(result[key], value)  # type: ignore[arg-type]
        else:
            result[key] = value  # type: ignore[assignment]
    return result


@dataclass(frozen=True)
class StorageAdapter:
    """Resolved configuration for a filesystem-backed location."""

    uri: str
    protocol: str
    storage_options: Dict[str, Any]
    reader_options: Dict[str, Any]
    writer_options: Dict[str, Any]

    def merge_reader_options(self, overrides: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        return _deep_merge(self.reader_options, overrides)

    def merge_writer_options(self, overrides: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        return _deep_merge(self.writer_options, overrides)


def build_storage_adapter(
    uri: Optional[str],
    env_cfg: Optional[Mapping[str, Any]],
    cfg_overrides: Optional[Mapping[str, Any]] = None,
) -> StorageAdapter:
    """Create a :class:`StorageAdapter` for ``uri``.

    Parameters
    ----------
    uri:
        Target URI. ``None`` or empty values yield a local ``file`` adapter.
    env_cfg:
        Environment configuration (typically ``config/env.yml`` contents).
    cfg_overrides:
        Optional mapping containing protocol specific overrides. Supported
        keys are ``storage_options``, ``reader_options`` and
        ``writer_options``.
    """

    normalized_uri = uri or ""
    protocol, _ = _split_uri(normalized_uri)

    env_cfg = env_cfg or {}
    cfg_overrides = cfg_overrides or {}

    storage_defaults = storage_options_from_env(normalized_uri, env_cfg)

    storage_section = (env_cfg.get("storage") or {}).get(protocol, {})
    storage_env_opts = storage_section.get("storage_options") or {}
    reader_env_opts = storage_section.get("reader_options") or {}
    writer_env_opts = storage_section.get("writer_options") or {}

    storage_cfg_opts = cfg_overrides.get("storage_options") or {}
    reader_cfg_opts = cfg_overrides.get("reader_options") or {}
    writer_cfg_opts = cfg_overrides.get("writer_options") or {}

    storage_options = _deep_merge(storage_defaults, storage_env_opts)
    storage_options = _deep_merge(storage_options, storage_cfg_opts)

    reader_options = _deep_merge(reader_env_opts, reader_cfg_opts)
    writer_options = _deep_merge(writer_env_opts, writer_cfg_opts)

    return StorageAdapter(
        uri=normalized_uri,
        protocol=protocol,
        storage_options=storage_options,
        reader_options=reader_options,
        writer_options=writer_options,
    )


__all__ = ["StorageAdapter", "build_storage_adapter"]
