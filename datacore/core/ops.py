"""Transformaciones declarativas puras."""

from __future__ import annotations

from typing import Any, Callable, Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import StructType

Operation = Callable[[DataFrame, Any], DataFrame]


def _ensure_iterable(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if value is None:
        return []
    return [str(value)]


def op_exclude(df: DataFrame, columns: list[str]) -> DataFrame:
    cols = _ensure_iterable(columns)
    if not cols:
        return df
    return df.drop(*cols)


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


def _normalize_whitespace(column: Column) -> Column:
    normalized = F.regexp_replace(column, r"\s+", " ")
    return F.trim(normalized)


def _remove_accents(col: Column) -> Column:
    """Remueve acentos mediante normalización unicode."""

    @F.udf("string")  # type: ignore[misc]
    def _strip_accents(value: str | None) -> str | None:  # pragma: no cover - udf ejecuta en Spark
        if value is None:
            return None
        import unicodedata

        normalized = unicodedata.normalize("NFD", value)
        return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")

    return _strip_accents(col)


def op_normalize(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    result = df
    for column in _ensure_iterable(config.get("trim")):
        result = result.withColumn(column, F.trim(F.col(column)))
    for column in _ensure_iterable(config.get("lower")):
        result = result.withColumn(column, F.lower(F.col(column)))
    for column in _ensure_iterable(config.get("upper")):
        result = result.withColumn(column, F.upper(F.col(column)))
    for column in _ensure_iterable(config.get("normalize_whitespace")):
        result = result.withColumn(column, _normalize_whitespace(F.col(column)))
    for column in _ensure_iterable(config.get("whitespace")):
        result = result.withColumn(column, _normalize_whitespace(F.col(column)))
    for column in _ensure_iterable(config.get("remove_accents")):
        result = result.withColumn(column, _remove_accents(F.col(column)))
    return result


def op_filter(df: DataFrame, expressions: Any) -> DataFrame:
    exprs = _ensure_iterable(expressions)
    result = df
    for expr in exprs:
        result = result.filter(F.expr(str(expr)))
    return result


def op_deduplicate(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    keys = _ensure_iterable(config.get("keys"))
    if not keys:
        return df
    order_by = _ensure_iterable(config.get("order_by")) or ["_ingestion_ts DESC"]

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
    into = config.get("into")
    explode_fn = F.explode_outer if outer else F.explode
    exploded = df.withColumn(into or column, explode_fn(F.col(column)))
    return exploded if into else exploded


def op_standardize_dates(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    cols = _ensure_iterable(config.get("cols"))
    if not cols:
        return df
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


def _flatten_once(
    df: DataFrame, prefix: str, targets: set[str] | None
) -> tuple[DataFrame, bool, set[str]]:
    struct_columns = [
        field
        for field in df.schema.fields
        if isinstance(field.dataType, StructType)
        and (not targets or field.name in targets)
    ]
    if not struct_columns:
        return df, False, set()
    remove = {field.name for field in struct_columns}
    select_exprs = [F.col(col) for col in df.columns if col not in remove]
    next_targets: set[str] = set()
    for field in struct_columns:
        base = field.name
        if prefix and not base.startswith(prefix):
            base = f"{prefix}{base}"
        for nested in field.dataType.fields:
            alias = f"{base}_{nested.name}".replace("__", "_")
            select_expr = F.col(f"{field.name}.{nested.name}").alias(alias)
            select_exprs.append(select_expr)
            if isinstance(nested.dataType, StructType):
                next_targets.add(alias)
    return df.select(*select_exprs), True, next_targets


def op_flatten(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    prefix = config.get("prefix", "")
    depth = config.get("depth")
    configured_targets = set(_ensure_iterable(config.get("json_cols")))
    result = df
    level = 0
    targets: set[str] | None = configured_targets or None
    while True:
        result, flattened, next_targets = _flatten_once(result, prefix, targets)
        if not flattened:
            break
        level += 1
        if depth is not None and level >= depth:
            break
        targets = next_targets if configured_targets else None
        if configured_targets and not targets:
            break
    return result


CANONICAL_ORDER = [
    "exclude",
    "rename",
    "cast",
    "normalize",
    "filter",
    "dedupe",
    "flatten",
    "explode",
    "sql",
]

OPERATIONS: dict[str, Operation] = {
    "exclude": op_exclude,
    "rename": op_rename,
    "cast": op_cast,
    "normalize": op_normalize,
    "filter": op_filter,
    "dedupe": op_deduplicate,
    "deduplicate": op_deduplicate,
    "explode": op_explode,
    "flatten": op_flatten,
    # compatibilidad histórica
    "drop_columns": op_exclude,
    "trim": lambda df, cols: op_normalize(df, {"trim": cols}),
    "uppercase": lambda df, cols: op_normalize(df, {"upper": cols}),
    "lowercase": lambda df, cols: op_normalize(df, {"lower": cols}),
    "normalize_whitespace": lambda df, cols: op_normalize(df, {"normalize_whitespace": cols}),
    "standardize_dates": op_standardize_dates,
    "flatten_json": op_flatten,
}

ALIASES = {
    "drop_columns": "exclude",
    "trim": "normalize",
    "uppercase": "normalize",
    "lowercase": "normalize",
    "normalize_whitespace": "normalize",
    "flatten_json": "flatten",
    "deduplicate": "dedupe",
    "standardize_dates": "normalize",
}


def _canonical_op(name: str) -> str:
    return ALIASES.get(name, name)


def apply_ops(df: DataFrame, ops: list[dict[str, Any] | str]) -> DataFrame:
    if not ops:
        return df

    buckets: dict[str, list[tuple[str, Any]]] = {name: [] for name in CANONICAL_ORDER}

    for op in ops:
        if isinstance(op, str):
            name, params = op, {}
        else:
            if len(op) != 1:
                raise ValueError(f"Las operaciones deben definirse como dicts de un solo elemento: {op}")
            name, params = next(iter(op.items()))
        canonical = _canonical_op(name)
        if canonical == "sql":
            buckets["sql"].append((name, params))
            continue
        if canonical not in OPERATIONS:
            raise KeyError(f"Operación declarativa no registrada: {name}")
        buckets.setdefault(canonical, [])
        buckets[canonical].append((name, params))

    result = df
    for stage in CANONICAL_ORDER:
        if stage == "sql":
            continue
        for name, params in buckets.get(stage, []):
            op_name = name if name in OPERATIONS else stage
            operation = OPERATIONS.get(op_name, OPERATIONS[stage])
            result = operation(result, params)

    # Operaciones SQL al final: cada entrada puede ser string o dict con "sql"
    for name, params in buckets.get("sql", []):
        statement = params if isinstance(params, str) else params.get("query") if isinstance(params, dict) else None
        if statement is None and isinstance(name, str) and name.lower() == "sql":
            statement = params
        if not statement:
            raise ValueError("Operación SQL sin sentencia definida")
        view_name = "__dc_ops"
        result.createOrReplaceTempView(view_name)
        result = result.sql_ctx.sql(statement)

    return result
