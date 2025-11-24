"""Extracción incremental básica para fuentes CDC."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from datacore.connectors.db import jdbc


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _build_filtered_source(config: dict[str, Any], predicate: str) -> dict[str, Any]:
    updated = dict(config)
    table = updated.get("table")
    query = updated.get("query")
    if query:
        updated["query"] = f"SELECT * FROM ({query}) base WHERE {predicate}"
    elif table:
        updated["query"] = f"SELECT * FROM {table} WHERE {predicate}"
        updated.pop("table", None)
    else:
        raise ValueError("CDC requiere 'table' o 'query'")
    return updated


def _max_value(df: DataFrame, column: str) -> Any:
    if column not in df.columns:
        return None
    result = df.agg(F.max(F.col(column)).alias("max_value")).collect()
    if not result:
        return None
    return result[0]["max_value"]


def incremental_fetch_jdbc(
    spark: SparkSession,
    source_cfg: dict[str, Any],
    cdc_cfg: dict[str, Any],
    last_state: dict[str, Any] | None,
) -> tuple[DataFrame, dict[str, Any]]:
    """Obtiene filas incrementales desde JDBC respetando la configuración CDC."""

    mode = cdc_cfg.get("mode")
    watermark_column = cdc_cfg.get("watermark_column")
    if mode in {"timestamp", "version_column"} and not watermark_column:
        raise ValueError("CDC requiere 'watermark_column' para modos timestamp/version_column")

    predicate: str | None = None
    state = dict(last_state or {})

    if mode == "timestamp" and state.get("watermark") and watermark_column:
        predicate = f"{watermark_column} > '{state['watermark']}'"
    elif mode == "version_column" and state.get("version") and watermark_column:
        predicate = f"{watermark_column} > {state['version']}"

    filtered_cfg = _build_filtered_source(source_cfg, predicate) if predicate else source_cfg
    df = jdbc.read(spark, filtered_cfg)

    if watermark_column:
        value = _max_value(df, watermark_column)
        if value is not None:
            key = "watermark" if mode == "timestamp" else "version"
            state[key] = _format_value(value)

    return df, state


__all__ = ["incremental_fetch_jdbc"]
