"""Operaciones de Warehouse para Microsoft Fabric."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable

from .env import FabricEnv

LOGGER = logging.getLogger(__name__)


def _sql_endpoint(env: FabricEnv) -> str:
    if not env.workspace_id or not env.warehouse_id:
        raise ValueError("workspace_id y warehouse_id son obligatorios para warehouse")
    return (
        "https://api.fabric.microsoft.com/v1/workspaces/"
        f"{env.workspace_id}/warehouses/{env.warehouse_id}/sql/statements"
    )


def _execute_sql(env: FabricEnv, statement: str) -> Dict[str, Any]:
    LOGGER.info(
        "Ejecutando SQL en Warehouse",
        extra={
            "action": "warehouse_sql",
            "workspace_id": env.workspace_id,
            "warehouse_id": env.warehouse_id,
        },
    )
    response = env.http("POST", _sql_endpoint(env), json={"statement": statement})
    response.raise_for_status()
    return response.json() if response.content else {"statement": statement}


def create_or_update_view(env: FabricEnv, delta_table: str, view_name: str, columns: Iterable[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    """Crea o actualiza una vista en el Warehouse apuntando a una tabla Delta."""

    if not delta_table or not view_name:
        raise ValueError("delta_table y view_name son obligatorios")
    select_expr = "*"
    if columns:
        casts = []
        for column in columns:
            name = column.get("name")
            dtype = column.get("type")
            if not name or not dtype:
                raise ValueError("Cada columna requiere 'name' y 'type'")
            casts.append(f"CAST([{name}] AS {dtype}) AS [{name}]")
        select_expr = ", ".join(casts)
    statement = (
        f"CREATE OR ALTER VIEW [{view_name}] AS SELECT {select_expr} FROM delta.`{delta_table}`"
    )
    result = _execute_sql(env, statement)
    result.update({"view": view_name, "statement": statement})
    return result


def copy_into(env: FabricEnv, table_name: str, source: str, *, file_format: str = "DELTA", options: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Ejecuta una operación COPY INTO con soporte de opciones comunes."""

    if not table_name or not source:
        raise ValueError("table_name y source son obligatorios")
    options = options or {}
    clauses = []
    if partition_by := options.get("partition"):  # pragma: no cover - depende del uso
        clauses.append(f"PARTITION = '{partition_by}'")
    if max_errors := options.get("max_errors"):
        clauses.append(f"MAXERRORS {int(max_errors)}")
    if pattern := options.get("pattern"):
        clauses.append(f"PATTERN = '{pattern}'")
    statement = f"COPY INTO [{table_name}] FROM '{source}' WITH (FILE_TYPE = '{file_format}'"
    if clauses:
        statement += ", " + ", ".join(clauses)
    statement += ")"
    result = _execute_sql(env, statement)
    result.update({"table": table_name, "statement": statement})
    return result
