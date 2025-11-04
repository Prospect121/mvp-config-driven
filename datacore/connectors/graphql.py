"""Compatibilidad para importaciones directas de GraphQL."""

from __future__ import annotations

from typing import Any, Iterable

from datacore.connectors.api.graphql import execute_query as _execute_query


def execute_query(config: dict[str, Any]) -> Iterable[dict[str, Any]]:
    return _execute_query(config)


__all__ = ["execute_query"]
