"""Orquestación de Spark Job Definitions y Data Pipelines en Fabric."""

from __future__ import annotations

import logging
from typing import Any, Dict

from .env import FabricEnv
from .security import build_auth_headers

LOGGER = logging.getLogger(__name__)


def _items_base(env: FabricEnv) -> str:
    if not env.workspace_id:
        raise ValueError("workspace_id es obligatorio para orquestación en Fabric")
    return f"https://api.fabric.microsoft.com/v1/workspaces/{env.workspace_id}/items"


def ensure_spark_job(env: FabricEnv, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Crea o actualiza una definición de Spark Job."""

    name = cfg.get("name")
    if not name:
        raise ValueError("Spark Job requiere 'name'")
    headers = build_auth_headers()
    payload = {
        "name": name,
        "type": "sparkJobDefinition",
        "properties": {
            "entryPoint": cfg.get("entrypoint"),
            "parameters": cfg.get("parameters", {}),
        },
    }
    LOGGER.info(
        "Asegurando Spark Job Definition %s en workspace %s",
        name,
        env.workspace_id,
    )
    response = env.http("POST", _items_base(env), json=payload, headers=headers)
    response.raise_for_status()
    body = response.json() if response.content else {}
    return {"id": body.get("id"), "url": body.get("webUrl"), "name": body.get("name", name)}


def ensure_pipeline(env: FabricEnv, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Crea o actualiza una Data Pipeline de Fabric."""

    name = cfg.get("name")
    if not name:
        raise ValueError("Pipeline requiere 'name'")
    headers = build_auth_headers()
    payload = {
        "name": name,
        "type": "dataPipeline",
        "properties": cfg.get("properties", {}),
    }
    LOGGER.info(
        "Asegurando Data Pipeline %s en workspace %s",
        name,
        env.workspace_id,
    )
    response = env.http("POST", _items_base(env), json=payload, headers=headers)
    response.raise_for_status()
    body = response.json() if response.content else {}
    return {"id": body.get("id"), "url": body.get("webUrl"), "name": body.get("name", name)}
