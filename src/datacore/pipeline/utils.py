"""Minimal shims for legacy pipeline helpers.

The monolithic ``pipelines`` package fue retirado. Estas utilidades existen
únicamente para avisar en tiempo de ejecución cuando alguien intenta usar el
flujo heredado.
"""
from __future__ import annotations

from typing import Any, Dict


def _legacy_removed(feature: str) -> RuntimeError:
    return RuntimeError(
        f"{feature} no está disponible: el monolito legacy fue eliminado. "
        "Usa `prodi run-layer` con transformaciones declarativas modernas."
    )


def run_raw_sources(cfg: Dict[str, Any], spark: Any, env: Dict[str, Any]) -> Any:  # pragma: no cover - legacy shim
    raise _legacy_removed("run_raw_sources")
