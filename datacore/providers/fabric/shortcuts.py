"""Gestión de OneLake Shortcuts mediante la API REST de Fabric."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Dict, Iterable, List, Optional

from .env import FabricEnv
from .security import build_auth_headers

LOGGER = logging.getLogger(__name__)


def fingerprint(cfg: Dict[str, Any]) -> str:
    payload = json.dumps(cfg, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _base_url(env: FabricEnv) -> str:
    if not env.workspace_id:
        raise ValueError("workspace_id es obligatorio para gestionar shortcuts en Fabric")
    return f"https://api.fabric.microsoft.com/v1/workspaces/{env.workspace_id}/shortcuts"


RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _request_with_retry(env: FabricEnv, method: str, url: str, *, retries: int = 3, **kwargs):
    backoff = 1.0
    attempt = 0
    while True:
        response = env.http(method, url, **kwargs)
        if response.status_code in RETRYABLE_STATUS and attempt < retries:
            LOGGER.warning(
                "Reintento HTTP Fabric",
                extra={
                    "action": "retry",
                    "attempt": attempt + 1,
                    "status": response.status_code,
                    "workspace_id": env.workspace_id,
                },
            )
            time.sleep(backoff)
            attempt += 1
            backoff *= 2
            continue
        response.raise_for_status()
        return response


def _list_shortcuts(env: FabricEnv) -> Iterable[Dict[str, Any]]:
    url = _base_url(env)
    items: List[Dict[str, Any]] = []
    continuation: Optional[str] = None
    while True:
        params = {"continuationToken": continuation} if continuation else None
        response = _request_with_retry(env, "GET", url, params=params)
        body = response.json() if response.content else {}
        batch = body.get("value", []) if isinstance(body, dict) else []
        items.extend(batch)
        continuation = body.get("continuationToken") if isinstance(body, dict) else None
        if not continuation:
            break
    return items


def _build_payload(sc_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": sc_cfg["name"],
        "type": sc_cfg.get("type"),
        "properties": {
            "target": sc_cfg.get("target"),
            "subpath": sc_cfg.get("subpath"),
            "labels": sc_cfg.get("labels", []),
            "fingerprint": fingerprint(sc_cfg),
        },
    }


def ensure_shortcut(env: FabricEnv, sc_cfg: Dict[str, Any]) -> None:
    """Crea o actualiza un shortcut en OneLake de forma idempotente."""

    if "name" not in sc_cfg:
        raise ValueError("La configuración de shortcut requiere 'name'")

    desired_fp = fingerprint(sc_cfg)
    payload = _build_payload(sc_cfg)
    headers = build_auth_headers()
    try:
        existing = next(
            (
                item
                for item in _list_shortcuts(env)
                if item.get("name") == sc_cfg["name"]
            ),
            None,
        )
    except Exception as exc:  # pragma: no cover - logging adicional
        LOGGER.error(
            "No se pudo listar shortcuts en Fabric (workspace=%s): %s",
            env.workspace_id,
            exc,
        )
        raise

    if not existing:
        LOGGER.info(
            "Creando shortcut Fabric",
            extra={
                "action": "create", "name": sc_cfg["name"], "workspace_id": env.workspace_id
            },
        )
        _request_with_retry(env, "POST", _base_url(env), json=payload, headers=headers)
        return

    current_fp = (existing.get("properties") or {}).get("fingerprint")
    shortcut_id = existing.get("id")
    if current_fp == desired_fp:
        LOGGER.debug(
            "Shortcut Fabric ya actualizado",
            extra={
                "action": "noop",
                "name": sc_cfg["name"],
                "workspace_id": env.workspace_id,
                "fingerprint": desired_fp,
            },
        )
        return

    LOGGER.info(
        "Actualizando shortcut Fabric",
        extra={
            "action": "update",
            "name": sc_cfg["name"],
            "workspace_id": env.workspace_id,
            "fingerprint": desired_fp,
        },
    )
    _request_with_retry(
        env,
        "PATCH",
        f"{_base_url(env)}/{shortcut_id}",
        json=payload,
        headers=headers,
    )
