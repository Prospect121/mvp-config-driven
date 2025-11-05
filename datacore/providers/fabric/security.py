"""Utilidades de seguridad y manejo de secretos para Fabric."""

from __future__ import annotations

from typing import Any, Dict


def build_auth_headers(extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Compone cabeceras adicionales para peticiones Fabric."""
    headers = {"User-Agent": "datacore-fabric/1.0"}
    if extra:
        headers.update(extra)
    return headers
