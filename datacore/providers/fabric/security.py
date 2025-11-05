"""Utilidades de seguridad y manejo de secretos para Fabric."""

from __future__ import annotations

from typing import Any, Dict

from datacore.utils.secrets import resolve_secret


def resolve_sensitive_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Resuelve URIs secret://* apoyándose en la capa de secretos."""

    resolved: Dict[str, Any] = {}
    for key, value in (payload or {}).items():
        if isinstance(value, str) and value.startswith("secret://"):
            resolved[key] = resolve_secret(value)
        else:
            resolved[key] = value
    return resolved


def build_auth_headers(extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Compone cabeceras adicionales para peticiones Fabric."""

    headers = {"User-Agent": "datacore-fabric/1.0"}
    if extra:
        headers.update(resolve_sensitive_values(extra))
    return headers
