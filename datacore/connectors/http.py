"""Conector HTTP con autenticación, reintentos y paginación."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)


class HttpError(RuntimeError):
    """Error controlado al consumir un endpoint HTTP."""


def _apply_auth(session: requests.Session, config: dict[str, Any]) -> dict[str, str]:
    headers = {**config.get("headers", {})}
    auth_conf = config.get("auth") or {}
    auth_type = (auth_conf.get("type") or "").lower()
    if auth_type == "bearer":
        token = auth_conf.get("token") or auth_conf.get("token_env")
        if token:
            headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "basic":
        user = auth_conf.get("username") or auth_conf.get("user")
        password = auth_conf.get("password")
        if user is not None and password is not None:
            session.auth = (user, password)
    return headers


def _extract_cursor(payload: Any, path: str | None) -> str | None:
    if not path:
        return None
    current = payload
    for segment in path.replace("$", "").strip(".").split("."):
        if not segment:
            continue
        if isinstance(current, dict):
            current = current.get(segment)
        else:
            return None
    if isinstance(current, str):
        return current or None
    return None


def _next_link_from_header(response: requests.Response) -> str | None:
    link_header = response.headers.get("Link")
    if not link_header:
        return None
    for part in link_header.split(","):
        section = part.strip()
        if "rel=\"next\"" in section:
            start = section.find("<")
            end = section.find(">", start + 1)
            if start != -1 and end != -1:
                return section[start + 1 : end]
    return None


def fetch_pages(config: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Genera páginas JSON aplicando autenticación, reintentos y paginación."""

    session = requests.Session()
    method = config.get("method", "GET").upper()
    url = config["url"]
    params = {**config.get("params", {})}
    body = config.get("body")
    pagination = config.get("pagination", {})
    strategy = (pagination.get("strategy") or "").lower()
    page_param = pagination.get("param", "page")
    cursor_path = pagination.get("cursor_path")
    max_pages = pagination.get("max_pages", 1 if not strategy else 1000)
    delay = float(config.get("retry", {}).get("backoff", pagination.get("backoff", 1.0)))
    max_attempts = int(config.get("retry", {}).get("max_attempts", 3))
    headers = _apply_auth(session, config)

    current_page = pagination.get("start", 1)
    next_cursor: str | None = pagination.get("start_cursor")
    next_url: str | None = None

    for _ in range(int(max_pages)):
        attempt = 0
        response: requests.Response | None = None
        while attempt < max_attempts:
            payload_params = {**params}
            if strategy == "page":
                payload_params[page_param] = current_page
            if strategy == "cursor" and next_cursor is not None:
                payload_params[page_param] = next_cursor
            target_url = next_url or url
            try:
                response = session.request(
                    method,
                    target_url,
                    headers=headers,
                    params=payload_params,
                    json=body if isinstance(body, dict) else None,
                    data=body if isinstance(body, str) else None,
                    timeout=config.get("timeout", 60),
                )
                if response.status_code >= 500:
                    raise HttpError(f"{response.status_code}: {response.text[:120]}")
                response.raise_for_status()
                break
            except Exception as exc:  # pragma: no cover - errores transitorios
                attempt += 1
                LOGGER.warning("Fallo HTTP %s (intento %s/%s)", exc, attempt, max_attempts)
                if attempt >= max_attempts:
                    raise
                time.sleep(delay * attempt)
        if response is None:
            raise HttpError("Respuesta HTTP vacía")
        data = response.json()
        yield data

        if strategy == "page":
            current_page += 1
            if pagination.get("stop_on_empty") and not data:
                break
        elif strategy == "cursor":
            next_cursor = _extract_cursor(data, cursor_path)
            if not next_cursor:
                break
        elif strategy == "link":
            next_url = _next_link_from_header(response)
            if not next_url:
                break
        else:
            break
