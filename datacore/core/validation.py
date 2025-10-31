"""Validaciones de datasets con métricas y rejects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)

ALIAS_MAP = {
    "expect_column_values_to_be_unique": "expect_unique",
}


@dataclass
class ValidationResult:
    """Resultado completo de validación."""

    valid_df: DataFrame
    invalid_df: DataFrame
    metrics: dict[str, Any]


def _normalize_rules(rules: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in rules.items():
        normalized_key = ALIAS_MAP.get(key, key)
        normalized[normalized_key] = value
    return normalized


def _not_null(df: DataFrame, column: str) -> tuple[DataFrame, str]:
    col_name = f"__rule_not_null_{column}"
    return df.withColumn(col_name, F.col(column).isNotNull()), col_name


def _unique(df: DataFrame, column: str) -> tuple[DataFrame, str]:
    col_name = f"__rule_unique_{column}"
    window = Window.partitionBy(F.col(column))
    df = df.withColumn(col_name, F.when(F.col(column).isNull(), F.lit(False)).otherwise(F.count("*").over(window) == 1))
    return df, col_name


def _domain(df: DataFrame, column: str, allowed: list[Any]) -> tuple[DataFrame, str]:
    col_name = f"__rule_domain_{column}"
    return df.withColumn(col_name, F.col(column).isin(*allowed)), col_name


def _between(df: DataFrame, config: dict[str, Any]) -> tuple[DataFrame, str]:
    column = config["col"]
    col_name = f"__rule_between_{column}"
    minimum = config.get("min")
    maximum = config.get("max")
    condition = F.lit(True)
    if minimum is not None:
        condition = condition & (F.col(column) >= F.lit(minimum))
    if maximum is not None:
        condition = condition & (F.col(column) <= F.lit(maximum))
    return df.withColumn(col_name, condition), col_name


def _regex(df: DataFrame, config: dict[str, Any]) -> tuple[DataFrame, str]:
    column = config["col"]
    pattern = config["pattern"]
    col_name = f"__rule_regex_{column}"
    return df.withColumn(col_name, F.col(column).rlike(pattern)), col_name


def _email(df: DataFrame, column: str) -> tuple[DataFrame, str]:
    col_name = f"__rule_email_{column}"
    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    return df.withColumn(col_name, F.col(column).rlike(pattern)), col_name


def _length(df: DataFrame, config: dict[str, Any]) -> tuple[DataFrame, str]:
    column = config["col"]
    col_name = f"__rule_length_{column}"
    min_length = config.get("min")
    max_length = config.get("max")
    length_col = F.length(F.col(column))
    condition = F.lit(True)
    if min_length is not None:
        condition = condition & (length_col >= F.lit(min_length))
    if max_length is not None:
        condition = condition & (length_col <= F.lit(max_length))
    return df.withColumn(col_name, condition), col_name


def _set_membership(df: DataFrame, config: dict[str, Any]) -> tuple[DataFrame, str]:
    column = config["col"]
    allowed = config.get("allowed", [])
    col_name = f"__rule_set_{column}"
    return df.withColumn(col_name, F.col(column).isin(*allowed)), col_name


def _foreign_key(df: DataFrame, config: dict[str, Any]) -> tuple[DataFrame, str]:
    column = config["col"]
    ref_table = config.get("ref_table")
    ref_col = config.get("ref_col")
    col_name = f"__rule_fk_{column}"
    if not ref_table or not ref_col:
        LOGGER.warning("Regla expect_foreign_key sin ref_table/ref_col, se omite")
        return df.withColumn(col_name, F.lit(True)), col_name
    try:
        reference = df.sparkSession.table(ref_table).select(F.col(ref_col).alias("__fk_ref")).distinct()
    except AnalysisException:
        LOGGER.warning("No se pudo resolver tabla de referencia %s, se omite validación", ref_table)
        return df.withColumn(col_name, F.lit(True)), col_name
    joined = df.join(reference, df[column] == reference["__fk_ref"], "left")
    result = joined.withColumn(col_name, reference["__fk_ref"].isNotNull()).drop("__fk_ref")
    return result, col_name


RULE_BUILDERS = {
    "expect_not_null": lambda df, cols: [ _not_null(df, column) for column in cols ],
}


def _apply_rules(df: DataFrame, rules: dict[str, Any]) -> tuple[DataFrame, list[str], list[tuple[str, str]]]:
    result = df
    rule_columns: list[str] = []
    rule_reasons: list[tuple[str, str]] = []

    for column in rules.get("expect_not_null", []):
        result, rule_col = _not_null(result, column)
        rule_columns.append(rule_col)
        rule_reasons.append((rule_col, f"expect_not_null:{column}"))

    for column in rules.get("expect_unique", []):
        result, rule_col = _unique(result, column)
        rule_columns.append(rule_col)
        rule_reasons.append((rule_col, f"expect_unique:{column}"))

    for column, allowed in rules.get("expect_domain", {}).items():
        result, rule_col = _domain(result, column, allowed)
        rule_columns.append(rule_col)
        rule_reasons.append((rule_col, f"expect_domain:{column}"))

    for config in rules.get("expect_between", []):
        result, rule_col = _between(result, config)
        rule_columns.append(rule_col)
        rule_reasons.append((rule_col, f"expect_between:{config['col']}"))

    for config in rules.get("expect_regex", []):
        result, rule_col = _regex(result, config)
        rule_columns.append(rule_col)
        rule_reasons.append((rule_col, f"expect_regex:{config['col']}"))

    for column in rules.get("expect_email", []):
        result, rule_col = _email(result, column)
        rule_columns.append(rule_col)
        rule_reasons.append((rule_col, f"expect_email:{column}"))

    for config in rules.get("expect_length", []):
        result, rule_col = _length(result, config)
        rule_columns.append(rule_col)
        rule_reasons.append((rule_col, f"expect_length:{config['col']}"))

    for config in rules.get("expect_set", []):
        result, rule_col = _set_membership(result, config)
        rule_columns.append(rule_col)
        rule_reasons.append((rule_col, f"expect_set:{config['col']}"))

    for config in rules.get("expect_foreign_key", []):
        result, rule_col = _foreign_key(result, config)
        rule_columns.append(rule_col)
        rule_reasons.append((rule_col, f"expect_foreign_key:{config['col']}"))

    return result, rule_columns, rule_reasons


def apply_validation(df: DataFrame, rules: dict[str, Any]) -> ValidationResult:
    normalized = _normalize_rules(rules)
    if not normalized:
        input_rows = df.count()
        metrics = {"input_rows": input_rows, "valid_rows": input_rows, "invalid_rows": 0, "rules": {}}
        return ValidationResult(valid_df=df, invalid_df=df.limit(0), metrics=metrics)

    with_checks, rule_columns, rule_reasons = _apply_rules(df, normalized)
    if not rule_columns:
        input_rows = df.count()
        metrics = {"input_rows": input_rows, "valid_rows": input_rows, "invalid_rows": 0, "rules": {}}
        return ValidationResult(valid_df=df, invalid_df=df.limit(0), metrics=metrics)

    validity_expr = F.lit(True)
    for column in rule_columns:
        validity_expr = validity_expr & F.col(column)
    enriched = with_checks.withColumn("__dc_valid", validity_expr)

    reasons = [F.when(~F.col(rule_col), F.lit(reason)) for rule_col, reason in rule_reasons]
    reasons = [expr for expr in reasons if expr is not None]
    if reasons:
        enriched = enriched.withColumn("__dc_reasons", F.array(*reasons))
        cleaned = F.array_distinct(F.expr("filter(__dc_reasons, x -> x is not null)"))
        enriched = enriched.withColumn(
            "_reject_reason",
            F.when(F.size(cleaned) > 0, F.array_join(cleaned, "; ")).otherwise(F.lit("")),
        )
    else:
        enriched = enriched.withColumn("_reject_reason", F.lit(""))

    input_rows = df.count()
    invalid_rows = enriched.filter(~F.col("__dc_valid")).count()
    valid_rows = input_rows - invalid_rows

    metrics_exprs = [
        F.sum(F.when(~F.col(column), F.lit(1)).otherwise(F.lit(0))).alias(reason)
        for column, reason in rule_reasons
    ]
    if metrics_exprs:
        metrics_row = enriched.agg(*metrics_exprs).collect()[0].asDict()
    else:
        metrics_row = {}

    metrics = {
        "input_rows": input_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "rules": metrics_row,
    }

    valid_df = enriched.filter(F.col("__dc_valid")).drop(*rule_columns, "__dc_valid", "__dc_reasons")
    invalid_df = enriched.filter(~F.col("__dc_valid")).drop(*rule_columns, "__dc_valid", "__dc_reasons")

    LOGGER.info("Validación completada. métricas=%s", metrics)
    return ValidationResult(valid_df=valid_df, invalid_df=invalid_df, metrics=metrics)
