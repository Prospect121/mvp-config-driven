"""Gestión de entorno y autenticación para Microsoft Fabric."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class FabricEnv:
    """Contexto de ejecución y utilidades HTTP para Microsoft Fabric."""

    workspace_id: Optional[str] = None
    environment_id: Optional[str] = None
    lakehouse_id: Optional[str] = None
    warehouse_id: Optional[str] = None
    eventhouse_id: Optional[str] = None
    tenant_id: Optional[str] = None
    _raw_cfg: Dict[str, Any] | None = None

    def __init__(self, cfg: Dict[str, Any]):
        object.__setattr__(self, "_raw_cfg", dict(cfg or {}))
        for key, value in (cfg or {}).items():
            object.__setattr__(self, key, value)

    # Métodos auxiliares -------------------------------------------------
    def get_token(self) -> str:
        """Obtiene un token AAD utilizando DefaultAzureCredential."""
        try:
            from azure.identity import DefaultAzureCredential
        except ImportError as exc:  # pragma: no cover - depende del extra opcional
            raise RuntimeError(
                "Falta azure-identity. Instala con: pip install .[fabric]"
            ) from exc
        credential = DefaultAzureCredential()
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
        return token.token

    def http(self, method: str, url: str, **kwargs):
        """Ejecuta una petición HTTP autenticada contra Fabric."""
        import requests

        headers = dict(kwargs.pop("headers", {}))
        headers.setdefault("Authorization", f"Bearer {self.get_token()}")
        headers.setdefault("Content-Type", "application/json")
        return requests.request(method, url, headers=headers, **kwargs)

    # Utilidades ---------------------------------------------------------
    def describe(self) -> Dict[str, Any]:
        """Devuelve información útil para logs y depuración."""
        return {k: getattr(self, k) for k in [
            "workspace_id",
            "environment_id",
            "lakehouse_id",
            "warehouse_id",
            "eventhouse_id",
            "tenant_id",
        ] if getattr(self, k) is not None}
