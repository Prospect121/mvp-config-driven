"""Transformaciones declarativas puras."""

from __future__ import annotations

from typing import Any, Callable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

Operation = Callable[[DataFrame, Any], DataFrame]


def op_drop_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.drop(*columns)


def op_rename(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    result = df
    for old, new in mapping.items():
        result = result.withColumnRenamed(old, new)
    return result


def op_cast(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    result = df
    for column, dtype in mapping.items():
        result = result.withColumn(column, F.col(column).cast(dtype))
    return result


def op_trim(df: DataFrame, columns: list[str]) -> DataFrame:
    result = df
    for column in columns:
        result = result.withColumn(column, F.trim(F.col(column)))
    return result


def op_uppercase(df: DataFrame, columns: list[str]) -> DataFrame:
    result = df
    for column in columns:
        result = result.withColumn(column, F.upper(F.col(column)))
    return result


def op_lowercase(df: DataFrame, columns: list[str]) -> DataFrame:
    result = df
    for column in columns:
        result = result.withColumn(column, F.lower(F.col(column)))
    return result


def op_normalize_whitespace(df: DataFrame, columns: list[str]) -> DataFrame:
    result = df
    for column in columns:
        normalized = F.regexp_replace(F.col(column), r"\s+", " ")
        result = result.withColumn(column, F.trim(normalized))
    return result


def op_standardize_dates(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    cols = config.get("cols", [])
    fmt_in = config.get("format_in")
    fmt_out = config.get("format_out", "yyyy-MM-dd HH:mm:ss")
    timezone = config.get("tz")
    result = df
    for column in cols:
        ts = F.to_timestamp(F.col(column), fmt_in) if fmt_in else F.to_timestamp(F.col(column))
        if timezone:
            ts = F.from_utc_timestamp(ts, timezone)
        result = result.withColumn(column, F.date_format(ts, fmt_out))
    return result


def op_deduplicate(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    keys = config.get("keys", [])
    order_by = config.get("order_by", ["_ingestion_ts DESC"])
    def _to_order(expr: str):
        parts = expr.strip().split()
        column = parts[0]
        direction = parts[1].lower() if len(parts) > 1 else "asc"
        col_expr = F.col(column)
        return col_expr.desc() if direction == "desc" else col_expr.asc()

    window = Window.partitionBy(*[F.col(key) for key in keys]).orderBy(*[_to_order(expr) for expr in order_by])
    ranked = df.withColumn("__dc_rn", F.row_number().over(window))
    return ranked.filter(F.col("__dc_rn") == 1).drop("__dc_rn")


def op_explode(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    column = config["col"]
    outer = bool(config.get("outer", False))
    explode_fn = F.explode_outer if outer else F.explode
    return df.withColumn(column, explode_fn(F.col(column)))


def _flatten_once(df: DataFrame, prefix: str) -> DataFrame:
    struct_columns = [field for field in df.schema.fields if isinstance(field.dataType, StructType)]
    if not struct_columns:
        return df
    select_exprs = [F.col(c) for c in df.columns if c not in {field.name for field in struct_columns}]
    for field in struct_columns:
        if prefix and not field.name.startswith(prefix):
            base = f"{prefix}{field.name}"
        else:
            base = field.name
        for nested in field.dataType.fields:
            alias = f"{base}_{nested.name}".replace("__", "_")
            select_exprs.append(F.col(f"{field.name}.{nested.name}").alias(alias))
    return df.select(*select_exprs)


def op_flatten_json(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    prefix = config.get("prefix", "")
    depth = config.get("depth")
    result = df
    level = 0
    while True:
        struct_columns = [field for field in result.schema.fields if isinstance(field.dataType, StructType)]
        if not struct_columns:
            break
        if depth is not None and level >= depth:
            break
        result = _flatten_once(result, prefix)
        level += 1
    return result


OPERATIONS: dict[str, Operation] = {
    "drop_columns": op_drop_columns,
    "rename": op_rename,
    "cast": op_cast,
    "trim": op_trim,
    "uppercase": op_uppercase,
    "lowercase": op_lowercase,
    "normalize_whitespace": op_normalize_whitespace,
    "standardize_dates": op_standardize_dates,
    "deduplicate": op_deduplicate,
    "explode": op_explode,
    "flatten_json": op_flatten_json,
}


def apply_ops(df: DataFrame, ops: list[dict[str, Any] | str]) -> DataFrame:
    result = df
    for op in ops:
        if isinstance(op, str):
            if op not in OPERATIONS:
                raise KeyError(f"Operación declarativa no registrada: {op}")
            result = OPERATIONS[op](result, {})
            continue
        if len(op) != 1:
            raise ValueError(f"Las operaciones deben definirse como dicts de un solo elemento: {op}")
        name, params = next(iter(op.items()))
        if name not in OPERATIONS:
            raise KeyError(f"Operación declarativa no registrada: {name}")
        result = OPERATIONS[name](result, params)
    return result
