"""Helpers to resolve layered configuration hierarchies."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

import yaml


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge two dictionaries returning a new mapping."""

    result: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = path.read_text(encoding="utf-8")
    data = yaml.safe_load(payload)
    if not isinstance(data, dict):
        return {}
    return data


def _ensure_platform_block(cfg: Dict[str, Any]) -> Dict[str, Any]:
    platform = cfg.get("platform")
    if isinstance(platform, str):
        block = {"name": platform}
        cfg = dict(cfg)
        cfg["platform"] = block
        return cfg
    if platform is None:
        cfg = dict(cfg)
        cfg["platform"] = {}
        return cfg
    return cfg


def _extract_platform_name(cfg: Dict[str, Any]) -> str | None:
    platform = cfg.get("platform")
    if isinstance(platform, dict):
        name = platform.get("name") or platform.get("id")
        if isinstance(name, str) and name.strip():
            return name.strip()
    elif isinstance(platform, str) and platform.strip():
        return platform.strip()
    return None


def resolve_hierarchical_layer_config(
    cfg: Dict[str, Any], *, source_path: Path | None = None
) -> Dict[str, Any]:
    """Merge defaults, platform overrides and the provided layer configuration."""

    if not isinstance(cfg, dict):
        return cfg

    config_root = Path("cfg")
    defaults_path = config_root / "defaults.yml"

    merged: Dict[str, Any] = {}
    defaults = _load_yaml(defaults_path)
    if defaults:
        merged = _deep_merge(merged, defaults)

    platform_name = _extract_platform_name(cfg) or _extract_platform_name(defaults)

    if platform_name:
        platform_cfg_path = config_root / "platforms" / f"{platform_name}.yml"
        platform_cfg = _load_yaml(platform_cfg_path)
        if platform_cfg:
            merged = _deep_merge(merged, platform_cfg)

    merged = _deep_merge(merged, cfg)
    merged = _ensure_platform_block(merged)

    return merged


__all__ = ["resolve_hierarchical_layer_config"]

