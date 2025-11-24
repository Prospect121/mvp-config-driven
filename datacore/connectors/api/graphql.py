"""Conector GraphQL."""

from __future__ import annotations

from typing import Any

import requests


def execute_query(config: dict[str, Any]) -> dict[str, Any]:
    url = config["url"]
    query = config["query"]
    variables = config.get("variables", {})
    headers = config.get("headers", {"Content-Type": "application/json"})
    response = requests.post(
        url,
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()
