"""Conector REST."""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Any

import requests


def fetch_pages(config: dict[str, Any]) -> Iterable[dict[str, Any]]:
    url = config["url"]
    headers = config.get("headers", {})
    params = config.get("params", {})
    page_param = config.get("page_param", "page")
    next_page = config.get("start_page", 1)
    max_pages = config.get("max_pages", 1)
    backoff = config.get("backoff", 1.0)
    token = config.get("bearer_token")
    if token:
        headers = {**headers, "Authorization": f"Bearer {token}"}
    for _ in range(max_pages):
        response = requests.get(
            url,
            headers=headers,
            params={**params, page_param: next_page},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        yield data
        next_page += 1
        time.sleep(backoff)
