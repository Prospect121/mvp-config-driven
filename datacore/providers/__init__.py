"""Gestión de providers opcionales."""

from __future__ import annotations

from typing import Any, Dict, Optional

from datacore.core.providers import Provider


def load_provider(global_cfg: Optional[Dict[str, Any]]) -> Optional[Provider]:
    """Carga condicional de providers en función de la configuración global."""
    providers_cfg = (global_cfg or {}).get("providers", {})
    if "fabric" in providers_cfg:
        try:
            from datacore.providers.fabric.provider import FabricProvider
        except ImportError as ex:  # pragma: no cover - mensaje claro para el usuario
            raise RuntimeError(
                "Provider 'fabric' requerido pero no instalado. "
                "Instala con: pip install .[fabric]"
            ) from ex
        return FabricProvider(providers_cfg["fabric"])
    return None


__all__ = ["Provider", "load_provider"]
