"""Operaciones de federación multi-fuente."""

from __future__ import annotations

from functools import reduce
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _resolve_source(
    dfs: dict[str, DataFrame], name: str, results: dict[str, DataFrame] = None
) -> DataFrame:
    """Resuelve una fuente por nombre, incluyendo resultados intermedios de joins."""
    if results and name in results:
        return results[name]
    if name not in dfs:
        raise KeyError(f"Fuente federada desconocida: {name}")
    return dfs[name]


def _apply_single_join(
    left_df: DataFrame,
    right_df: DataFrame,
    left_name: str,
    right_name: str,
    on_pairs: list[dict],
    join_type: str,
    select_conf: list[dict] = None,
) -> DataFrame:
    """Ejecuta un join simple entre dos DataFrames usando sus nombres como alias."""
    if not on_pairs:
        raise ValueError("La federación join requiere condición 'on'")

    # Usar nombres de fuentes como alias (más intuitivo y escalable)
    left_aliased = left_df.alias(left_name)
    right_aliased = right_df.alias(right_name)

    conditions = []
    for pair in on_pairs:
        left_col = pair.get("left")
        right_col = pair.get("right")
        if not left_col or not right_col:
            raise ValueError("Par de join inválido, requiere 'left' y 'right'")
        conditions.append(F.col(f"{left_name}.{left_col}") == F.col(f"{right_name}.{right_col}"))

    join_condition = reduce(lambda acc, expr: acc & expr, conditions)
    joined = left_aliased.join(right_aliased, on=join_condition, how=join_type)

    if not select_conf:
        return joined

    projections = [F.expr(rule["expr"]).alias(rule["as"]) for rule in select_conf]
    return joined.select(*projections)


def _apply_join(dfs: dict[str, DataFrame], config: dict[str, Any]) -> DataFrame:
    """
    Aplica joins simples o encadenados.

    Soporta dos formatos:
    1. Join simple (2 tablas):
       join:
         left: orders
         right: customers
         on: [{left: customer_id, right: customer_id}]

    2. Joins encadenados (3+ tablas):
       join:
         chain:
           - left: orders
             right: customers
             on: [{left: customer_id, right: customer_id}]
             as: orders_customers
           - left: orders_customers
             right: products
             on: [{left: product_id, right: id}]
    """
    # Verificar si es join encadenado
    chain = config.get("chain")
    if chain:
        return _apply_chained_joins(dfs, chain, config.get("select"))

    # Join simple (mantener compatibilidad)
    left_name = config["left"]
    right_name = config["right"]
    on_pairs = config.get("on") or []
    join_type = config.get("join_type", "inner")
    select_conf = config.get("select") or []

    left_df = _resolve_source(dfs, left_name)
    right_df = _resolve_source(dfs, right_name)

    return _apply_single_join(
        left_df,
        right_df,
        left_name,
        right_name,
        on_pairs,
        join_type,
        select_conf if select_conf else None,
    )


def _apply_chained_joins(
    dfs: dict[str, DataFrame],
    chain: list[dict],
    final_select: list[dict] = None,
) -> DataFrame:
    """
    Ejecuta una cadena de joins secuenciales.

    Cada paso puede referenciar fuentes originales o resultados
    intermedios usando el campo 'as'.
    """
    if not chain:
        raise ValueError("La cadena de joins está vacía")

    results: dict[str, DataFrame] = {}
    current_result = None

    for idx, step in enumerate(chain):
        left_name = step["left"]
        right_name = step["right"]
        on_pairs = step.get("on") or []
        join_type = step.get("join_type", "inner")
        step_select = step.get("select")
        result_name = step.get("as", f"_chain_result_{idx}")

        # Resolver fuentes (pueden ser originales o resultados intermedios)
        left_df = _resolve_source(dfs, left_name, results)
        right_df = _resolve_source(dfs, right_name, results)

        current_result = _apply_single_join(
            left_df, right_df, left_name, right_name, on_pairs, join_type, step_select
        )

        # Guardar resultado intermedio para siguientes pasos
        results[result_name] = current_result

    # Aplicar select final si existe
    if final_select and current_result is not None:
        projections = [F.expr(rule["expr"]).alias(rule["as"]) for rule in final_select]
        return current_result.select(*projections)

    return current_result


def _apply_union(dfs: dict[str, DataFrame], config: dict[str, Any]) -> DataFrame:
    frames = list(dfs.values())
    if not frames:
        raise ValueError("No existen fuentes para unir")
    allow_missing = bool(config.get("allow_missing_columns", True))
    schema_mode = config.get("schema_mode", "by_name")
    result = frames[0]
    for frame in frames[1:]:
        if schema_mode == "by_position":
            result = result.union(frame)
        else:
            result = result.unionByName(frame, allowMissingColumns=allow_missing)
    return result


def apply_federation(dfs: dict[str, DataFrame], config: dict[str, Any]) -> DataFrame:
    """Aplica la estrategia de federación configurada sobre los DataFrames."""

    if not config:
        raise ValueError("Se requiere configuración de federación")
    strategy = config.get("strategy")
    if strategy == "join":
        return _apply_join(dfs, config.get("join", {}))
    if strategy == "union":
        return _apply_union(dfs, config.get("union", {}))
    raise ValueError(f"Estrategia de federación no soportada: {strategy}")


__all__ = ["apply_federation"]
