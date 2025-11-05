"""Resolución de shortcuts de datos sin copia."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession

from datacore.platforms.base import PlatformBase
from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)


def _registry_path(source: dict[str, Any], platform: PlatformBase) -> Path:
    candidate = source.get("registry") or platform.config.get("shortcuts_registry")
    if candidate:
        return Path(candidate)
    return Path(".prodi/shortcuts.yml")


@lru_cache(maxsize=8)
def _load_registry(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        LOGGER.warning("Registro de shortcuts %s no encontrado", path)
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:  # pragma: no cover - error de formato
        raise ValueError(f"YAML de shortcuts inválido: {path}") from exc
    if not payload:
        return {}
    if isinstance(payload, list):
        # permitir lista de pares name/provider
        registry: dict[str, dict[str, Any]] = {}
        for item in payload:
            if not isinstance(item, dict) or "name" not in item:
                continue
            name = str(item["name"])
            registry[name] = {key: value for key, value in item.items() if key != "name"}
        return registry
    if not isinstance(payload, dict):
        raise ValueError("El archivo shortcuts debe ser un objeto mapping")
    result: dict[str, dict[str, Any]] = {}
    for name, conf in payload.items():
        if conf is None:
            result[str(name)] = {}
        elif isinstance(conf, dict):
            result[str(name)] = dict(conf)
        else:
            raise ValueError(f"Shortcut {name} debe ser un objeto con configuración")
    return result


def _normalize_source_config(
    source: dict[str, Any],
    base: dict[str, Any],
) -> dict[str, Any]:
    merged = {**base}
    overrides = {key: value for key, value in source.items() if key not in {"type", "name"}}
    merged.update(overrides)
    provider = merged.get("provider") or merged.get("type")
    if not provider:
        raise ValueError(f"Shortcut {source.get('name')} sin proveedor definido")
    provider = str(provider)
    merged["type"] = provider
    target_uri = merged.pop("target_uri", None)
    if target_uri and provider == "storage":
        merged.setdefault("uri", target_uri)
    elif target_uri and provider in {"jdbc", "api_rest", "api_graphql", "endpoint"}:
        merged.setdefault("url", target_uri)
    read_options = merged.get("read_options")
    if isinstance(read_options, str):
        try:
            merged["read_options"] = json.loads(read_options)
        except json.JSONDecodeError:
            LOGGER.warning("read_options de shortcut %s no es JSON válido", source.get("name"))
            merged["read_options"] = {}
    return merged


def resolve_shortcut(
    source: dict[str, Any],
    platform: PlatformBase,
    spark: SparkSession,
    *,
    layer: str,
    dataset: str,
    environment: str,
) -> DataFrame:
    """Resuelve un shortcut en un DataFrame sin ingestión adicional."""

    name = source.get("name")
    if not name:
        raise ValueError("El shortcut requiere atributo 'name'")
    registry_file = _registry_path(source, platform)
    registry = _load_registry(registry_file)
    base_conf = registry.get(name, {})
    if not base_conf:
        LOGGER.warning("Shortcut %s no encontrado en %s", name, registry_file)
    merged = _normalize_source_config(source, base_conf)
    provider = merged.get("type")
    if provider == "shortcut":
        raise ValueError("Los shortcuts no pueden referenciar otros shortcuts")
    LOGGER.info(
        "Resolviendo shortcut %s con proveedor %s hacia %s",
        name,
        provider,
        merged.get("uri") or merged.get("url"),
    )
    from datacore.io import readers  # import tardío para evitar ciclos

    return readers.read_batch(
        spark,
        platform,
        merged,
        layer=layer,
        dataset=dataset,
        environment=environment,
    )


__all__ = ["resolve_shortcut"]
