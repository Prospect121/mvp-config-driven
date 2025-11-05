"""Gestión de entorno y autenticación para Microsoft Fabric."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

LOGGER = logging.getLogger(__name__)

PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")
ALLOWED_PLACEHOLDERS = {
    "FABRIC_WORKSPACE_ID",
    "FABRIC_ENVIRONMENT_ID",
    "FABRIC_LAKEHOUSE_ID",
    "FABRIC_WAREHOUSE_ID",
    "FABRIC_EVENTHOUSE_ID",
    "FABRIC_TENANT_ID",
}


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
            if isinstance(value, str):
                value = self._resolve_placeholders(key, value)
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
        if os.getenv("FABRIC_HTTP_OFFLINE") == "1":  # pragma: no cover - solo para entornos locales
            payload = kwargs.get("json") or {"value": []}

            class _OfflineResponse:
                status_code = 200

                def __init__(self, data):
                    import json as _json

                    self._data = data
                    self.content = _json.dumps(data).encode("utf-8")

                def json(self):
                    return self._data

                def raise_for_status(self):
                    return None

            return _OfflineResponse(payload)

        import requests

        headers = dict(kwargs.pop("headers", {}))
        headers.setdefault("Authorization", f"Bearer {self.get_token()}")
        headers.setdefault("Content-Type", "application/json")
        response = requests.request(method, url, headers=headers, **kwargs)
        return response

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

    # Internos -----------------------------------------------------------
    def _resolve_placeholders(self, field: str, value: str) -> str:
        matches = PLACEHOLDER_PATTERN.findall(value)
        if not matches:
            return value
        resolved = value
        for placeholder in matches:
            if placeholder not in ALLOWED_PLACEHOLDERS:
                raise RuntimeError(
                    f"Placeholder {placeholder} no permitido en providers.fabric.{field}"
                )
            env_value = os.getenv(placeholder)
            if env_value is None:
                raise RuntimeError(
                    f"Variable de entorno {placeholder} requerida para providers.fabric.{field}"
                )
            LOGGER.info(
                "Resolviendo placeholder Fabric",
                extra={
                    "field": field,
                    "placeholder": placeholder,
                    "workspace_id": getattr(self, "workspace_id", None),
                },
            )
            resolved = resolved.replace(f"${{{placeholder}}}", env_value)
        return resolved
