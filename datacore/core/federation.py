"""Operaciones de federación multi-fuente."""

from __future__ import annotations

from functools import reduce
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _resolve_source(dfs: dict[str, DataFrame], name: str) -> DataFrame:
    if name not in dfs:
        raise KeyError(f"Fuente federada desconocida: {name}")
    return dfs[name]


def _apply_join(dfs: dict[str, DataFrame], config: dict[str, Any]) -> DataFrame:
    left_name = config["left"]
    right_name = config["right"]
    on_pairs = config.get("on") or []
    join_type = config.get("join_type", "inner")
    if not on_pairs:
        raise ValueError("La federación join requiere condición 'on'")
    left_df = _resolve_source(dfs, left_name).alias("left")
    right_df = _resolve_source(dfs, right_name).alias("right")

    conditions = []
    for pair in on_pairs:
        left_col = pair.get("left")
        right_col = pair.get("right")
        if not left_col or not right_col:
            raise ValueError("Par de join inválido, requiere 'left' y 'right'")
        conditions.append(F.col(f"left.{left_col}") == F.col(f"right.{right_col}"))

    join_condition = reduce(lambda acc, expr: acc & expr, conditions)
    joined = left_df.join(right_df, on=join_condition, how=join_type)

    select_conf = config.get("select") or []
    if not select_conf:
        return joined

    projections = [F.expr(rule["expr"]).alias(rule["as"]) for rule in select_conf]
    return joined.select(*projections)


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
