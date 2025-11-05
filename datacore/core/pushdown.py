"""Heurísticas simples para pushdown de filtros/proyecciones."""

from __future__ import annotations

import re
from typing import Any, Iterable

ALLOWED_TOKENS = re.compile(r"^[\w\s=<>\.'""%(),+-]+$")


def _extract_filters(ops: Iterable[Any]) -> list[str]:
    filters: list[str] = []
    for op in ops:
        if isinstance(op, dict) and "filter" in op:
            value = op["filter"]
        elif isinstance(op, dict) and "where" in op:
            value = op["where"]
        else:
            continue
        if isinstance(value, str):
            filters.append(value)
        elif isinstance(value, (list, tuple)):
            filters.extend(str(expr) for expr in value)
    return filters


def _safe_expression(expr: str) -> bool:
    return bool(ALLOWED_TOKENS.match(expr.strip()))


def try_pushdown(source_cfg: dict[str, Any], ops_cfg: list[Any]) -> str | None:
    """Devuelve SQL a ejecutar en origen cuando es seguro aplicar pushdown."""

    if source_cfg.get("type") != "jdbc":
        return None
    table = source_cfg.get("table")
    if not table:
        return None
    filters = _extract_filters(ops_cfg)
    if not filters:
        return None
    if not all(_safe_expression(expr) for expr in filters):
        return None
    where_clause = " AND ".join(f"({expr})" for expr in filters)
    return f"SELECT * FROM {table} WHERE {where_clause}"


__all__ = ["try_pushdown"]
