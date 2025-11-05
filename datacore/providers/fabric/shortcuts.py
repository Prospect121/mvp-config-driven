"""Gestión de OneLake Shortcuts mediante la API REST de Fabric."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Iterable

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


def _list_shortcuts(env: FabricEnv) -> Iterable[Dict[str, Any]]:
    response = env.http("GET", _base_url(env))
    response.raise_for_status()
    body = response.json() if response.content else {}
    items = body.get("value", []) if isinstance(body, dict) else []
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
            "Creando shortcut %s en workspace %s",
            sc_cfg["name"],
            env.workspace_id,
        )
        response = env.http("POST", _base_url(env), json=payload, headers=headers)
        response.raise_for_status()
        return

    current_fp = (existing.get("properties") or {}).get("fingerprint")
    shortcut_id = existing.get("id")
    if current_fp == desired_fp:
        LOGGER.debug(
            "Shortcut %s ya está actualizado (fingerprint %s)",
            sc_cfg["name"],
            desired_fp,
        )
        return

    LOGGER.info(
        "Actualizando shortcut %s en workspace %s",
        sc_cfg["name"],
        env.workspace_id,
    )
    response = env.http(
        "PATCH",
        f"{_base_url(env)}/{shortcut_id}",
        json=payload,
        headers=headers,
    )
    response.raise_for_status()
