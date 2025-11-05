"""Orquestación de Spark Job Definitions y Data Pipelines en Fabric."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict

from .env import FabricEnv
from .shortcuts import fingerprint

LOGGER = logging.getLogger(__name__)


RETRYABLE = {429, 500, 502, 503, 504}


def _items_base(env: FabricEnv) -> str:
    if not env.workspace_id:
        raise ValueError("workspace_id es obligatorio para orquestación en Fabric")
    return f"https://api.fabric.microsoft.com/v1/workspaces/{env.workspace_id}/items"


def _request(env: FabricEnv, method: str, url: str, *, retries: int = 3, **kwargs):
    attempt = 0
    wait = 1.0
    while True:
        response = env.http(method, url, **kwargs)
        if response.status_code in RETRYABLE and attempt < retries:
            LOGGER.warning(
                "Reintento orquestación Fabric",
                extra={
                    "action": "retry",
                    "attempt": attempt + 1,
                    "status": response.status_code,
                    "workspace_id": env.workspace_id,
                },
            )
            time.sleep(wait)
            wait *= 2
            attempt += 1
            continue
        response.raise_for_status()
        return response


def _find_item(env: FabricEnv, name: str, item_type: str) -> Dict[str, Any] | None:
    response = _request(
        env,
        "GET",
        _items_base(env),
        params={"$filter": f"name eq '{name}' and type eq '{item_type}'"},
    )
    body = response.json() if response.content else {}
    items = body.get("value", []) if isinstance(body, dict) else []
    return items[0] if items else None


def ensure_spark_job(env: FabricEnv, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Crea o actualiza una definición de Spark Job."""

    name = cfg.get("name")
    if not name:
        raise ValueError("Spark Job requiere 'name'")
    payload = {
        "name": name,
        "type": "sparkJobDefinition",
        "properties": {
            "entryPoint": cfg.get("entrypoint"),
            "parameters": cfg.get("parameters", {}),
            "fingerprint": fingerprint(cfg),
        },
    }
    LOGGER.info(
        "Asegurando Spark Job Definition",
        extra={
            "action": "ensure_spark_job",
            "name": name,
            "workspace_id": env.workspace_id,
        },
    )
    existing = _find_item(env, name, "sparkJobDefinition")
    desired_fp = payload["properties"]["fingerprint"]
    if existing:
        current_fp = (existing.get("properties") or {}).get("fingerprint")
        if current_fp == desired_fp:
            LOGGER.debug(
                "Spark Job ya actualizado",
                extra={
                    "action": "noop",
                    "name": name,
                    "workspace_id": env.workspace_id,
                },
            )
            return {
                "id": existing.get("id"),
                "url": existing.get("webUrl"),
                "name": existing.get("name", name),
            }
        response = _request(
            env,
            "PATCH",
            f"{_items_base(env)}/{existing.get('id')}",
            json=payload,
        )
        body = response.json() if response.content else {}
        return {
            "id": body.get("id", existing.get("id")),
            "url": body.get("webUrl", existing.get("webUrl")),
            "name": body.get("name", name),
        }
    response = _request(env, "POST", _items_base(env), json=payload)
    body = response.json() if response.content else {}
    return {"id": body.get("id"), "url": body.get("webUrl"), "name": body.get("name", name)}


def ensure_pipeline(env: FabricEnv, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Crea o actualiza una Data Pipeline de Fabric."""

    name = cfg.get("name")
    if not name:
        raise ValueError("Pipeline requiere 'name'")
    properties = cfg.get("properties", {})
    payload = {
        "name": name,
        "type": "dataPipeline",
        "properties": {
            **properties,
            "fingerprint": fingerprint(cfg),
        },
    }
    LOGGER.info(
        "Asegurando Data Pipeline",
        extra={
            "action": "ensure_pipeline",
            "name": name,
            "workspace_id": env.workspace_id,
        },
    )
    existing = _find_item(env, name, "dataPipeline")
    desired_fp = payload["properties"]["fingerprint"]
    if existing:
        current_fp = (existing.get("properties") or {}).get("fingerprint")
        if current_fp == desired_fp:
            LOGGER.debug(
                "Pipeline ya actualizada",
                extra={
                    "action": "noop",
                    "name": name,
                    "workspace_id": env.workspace_id,
                },
            )
            return {
                "id": existing.get("id"),
                "url": existing.get("webUrl"),
                "name": existing.get("name", name),
            }
        response = _request(
            env,
            "PATCH",
            f"{_items_base(env)}/{existing.get('id')}",
            json=payload,
        )
        body = response.json() if response.content else {}
        return {
            "id": body.get("id", existing.get("id")),
            "url": body.get("webUrl", existing.get("webUrl")),
            "name": body.get("name", name),
        }
    response = _request(env, "POST", _items_base(env), json=payload)
    body = response.json() if response.content else {}
    return {"id": body.get("id"), "url": body.get("webUrl"), "name": body.get("name", name)}
