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
    "expect_domain": "values_in_set",
    "expect_set": "values_in_set",
    "expect_between": "range",
    "expect_regex": "regex",
}


@dataclass
class ValidationResult:
    """Resultado completo de validación."""

    valid_df: DataFrame
    invalid_df: DataFrame
    quarantine_df: DataFrame
    metrics: dict[str, Any]
    extras: dict[str, Any] | None = None


@dataclass
class RuleConfig:
    name: str
    identifier: str
    params: dict[str, Any]
    severity: str = "error"
    threshold: float = 0.0
    on_fail: str | None = None


def _not_null(df: DataFrame, column: str) -> tuple[DataFrame, str]:
    col_name = f"__rule_not_null_{column}"
    return df.withColumn(col_name, F.col(column).isNotNull()), col_name


def _unique(df: DataFrame, column: str) -> tuple[DataFrame, str]:
    col_name = f"__rule_unique_{column}"
    window = Window.partitionBy(F.col(column))
    df = df.withColumn(col_name, F.when(F.col(column).isNull(), F.lit(False)).otherwise(F.count("*").over(window) == 1))
    return df, col_name


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
    "expect_not_null": lambda df, column: _not_null(df, column),
    "expect_unique": lambda df, column: _unique(df, column),
    "values_in_set": lambda df, conf: _set_membership(df, conf),
    "range": lambda df, conf: _between(df, conf),
    "regex": lambda df, conf: _regex(df, conf),
    "expect_email": lambda df, column: _email(df, column),
    "expect_length": lambda df, conf: _length(df, conf),
    "expect_foreign_key": lambda df, conf: _foreign_key(df, conf),
}


def _ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _legacy_to_rules(config: dict[str, Any]) -> list[RuleConfig]:
    rules: list[RuleConfig] = []
    normalized = {ALIAS_MAP.get(key, key): value for key, value in config.items() if key != "quarantine_sink"}
    for column in _ensure_list(normalized.get("expect_not_null")):
        rules.append(RuleConfig("expect_not_null", f"expect_not_null:{column}", {"column": column}))
    for column in _ensure_list(normalized.get("expect_unique")):
        rules.append(RuleConfig("expect_unique", f"expect_unique:{column}", {"column": column}))
    domain_conf = normalized.get("values_in_set") or normalized.get("expect_set") or {}
    if isinstance(domain_conf, dict):
        items = domain_conf.items()
    else:
        items = []
        for entry in _ensure_list(domain_conf):
            if isinstance(entry, dict) and "col" in entry:
                items.append((entry.get("col"), entry.get("allowed")))
    for column, allowed in items:
        if column is None:
            continue
        rules.append(
            RuleConfig("values_in_set", f"values_in_set:{column}", {"col": column, "allowed": allowed})
        )
    between_conf = normalized.get("range") or normalized.get("expect_between") or []
    for entry in _ensure_list(between_conf):
        if isinstance(entry, dict) and "col" in entry:
            rules.append(RuleConfig("range", f"range:{entry['col']}", entry))
    regex_conf = normalized.get("regex") or normalized.get("expect_regex") or []
    for entry in _ensure_list(regex_conf):
        if isinstance(entry, dict) and "col" in entry:
            rules.append(RuleConfig("regex", f"regex:{entry['col']}", entry))
    for column in _ensure_list(normalized.get("expect_email")):
        rules.append(RuleConfig("expect_email", f"expect_email:{column}", {"column": column}))
    length_conf = normalized.get("expect_length") or []
    for entry in _ensure_list(length_conf):
        if isinstance(entry, dict) and "col" in entry:
            rules.append(RuleConfig("expect_length", f"expect_length:{entry['col']}", entry))
    fk_conf = normalized.get("expect_foreign_key") or []
    for entry in _ensure_list(fk_conf):
        if isinstance(entry, dict) and "col" in entry:
            rules.append(RuleConfig("expect_foreign_key", f"expect_foreign_key:{entry['col']}", entry))
    return rules


def _modern_to_rules(config: dict[str, Any]) -> list[RuleConfig]:
    rules: list[RuleConfig] = []
    for entry in config.get("rules", []):
        if not isinstance(entry, dict):
            continue
        check = ALIAS_MAP.get(entry.get("check"), entry.get("check"))
        if not check:
            continue
        severity = str(entry.get("severity", "error")).lower()
        threshold = float(entry.get("threshold", 0.0))
        on_fail = entry.get("on_fail")
        identifier = entry.get("name")
        if check in {"expect_not_null", "expect_unique", "expect_email"}:
            for column in _ensure_list(entry.get("columns") or entry.get("column")):
                rule_id = identifier or f"{check}:{column}"
                params = {"column": column}
                rules.append(
                    RuleConfig(check, rule_id, params, severity=severity, threshold=threshold, on_fail=on_fail)
                )
        elif check in {"values_in_set"}:
            column = entry.get("column") or entry.get("col")
            allowed = entry.get("allowed") or entry.get("values")
            if column is not None:
                rule_id = identifier or f"{check}:{column}"
                params = {"col": column, "allowed": allowed}
                rules.append(
                    RuleConfig(check, rule_id, params, severity=severity, threshold=threshold, on_fail=on_fail)
                )
        elif check in {"range"}:
            column = entry.get("column") or entry.get("col")
            if column is not None:
                rule_id = identifier or f"{check}:{column}"
                params = {"col": column, "min": entry.get("min"), "max": entry.get("max")}
                rules.append(
                    RuleConfig(check, rule_id, params, severity=severity, threshold=threshold, on_fail=on_fail)
                )
        elif check in {"regex"}:
            column = entry.get("column") or entry.get("col")
            pattern = entry.get("pattern")
            if column is not None and pattern is not None:
                rule_id = identifier or f"{check}:{column}"
                params = {"col": column, "pattern": pattern}
                rules.append(
                    RuleConfig(check, rule_id, params, severity=severity, threshold=threshold, on_fail=on_fail)
                )
        elif check in {"expect_length"}:
            column = entry.get("column") or entry.get("col")
            if column is not None:
                rule_id = identifier or f"{check}:{column}"
                params = {
                    "col": column,
                    "min": entry.get("min"),
                    "max": entry.get("max"),
                }
                rules.append(
                    RuleConfig(check, rule_id, params, severity=severity, threshold=threshold, on_fail=on_fail)
                )
        elif check in {"expect_foreign_key"}:
            column = entry.get("column") or entry.get("col")
            if column is not None:
                rule_id = identifier or f"{check}:{column}"
                params = {
                    "col": column,
                    "ref_table": entry.get("ref_table"),
                    "ref_col": entry.get("ref_col"),
                }
                rules.append(
                    RuleConfig(check, rule_id, params, severity=severity, threshold=threshold, on_fail=on_fail)
                )
    return rules


def _extract_rules(config: dict[str, Any]) -> tuple[list[RuleConfig], dict[str, Any]]:
    if not config:
        return [], {}
    if "rules" in config:
        rules = _modern_to_rules(config)
    else:
        rules = _legacy_to_rules(config)
    extras = {}
    if "quarantine_sink" in config:
        extras["quarantine_sink"] = config["quarantine_sink"]
    return rules, extras


def apply_validation(df: DataFrame, rules: dict[str, Any]) -> ValidationResult:
    rule_configs, extras = _extract_rules(rules)
    if not rule_configs:
        input_rows = df.count()
        empty = df.limit(0)
        metrics = {"input_rows": input_rows, "valid_rows": input_rows, "invalid_rows": 0, "rules": {}}
        return ValidationResult(
            valid_df=df,
            invalid_df=empty,
            quarantine_df=empty,
            metrics=metrics,
            extras=extras,
        )

    enriched = df
    runtime: list[tuple[RuleConfig, str]] = []
    for rule in rule_configs:
        builder = RULE_BUILDERS.get(rule.name)
        if not builder:
            LOGGER.warning("Regla %s no soportada, se omite", rule.name)
            continue
        if rule.name in {"expect_not_null", "expect_unique", "expect_email"}:
            enriched, column = builder(enriched, rule.params["column"])
        else:
            enriched, column = builder(enriched, rule.params)
        runtime.append((rule, column))

    if not runtime:
        input_rows = df.count()
        empty = df.limit(0)
        metrics = {"input_rows": input_rows, "valid_rows": input_rows, "invalid_rows": 0, "rules": {}}
        return ValidationResult(
            valid_df=df,
            invalid_df=empty,
            quarantine_df=empty,
            metrics=metrics,
            extras=extras,
        )

    input_rows = df.count()
    metrics_exprs = [
        F.sum(F.when(~F.col(column), F.lit(1)).otherwise(F.lit(0))).alias(column) for _, column in runtime
    ]
    counts_row = enriched.agg(*metrics_exprs).collect()[0] if metrics_exprs else None

    rule_metrics: dict[str, Any] = {}
    reject_reasons: list[tuple[str, str]] = []
    quarantine_frames: list[DataFrame] = []

    for rule, column in runtime:
        invalid_count = counts_row[column] if counts_row else 0
        ratio = (invalid_count / input_rows) if input_rows else 0.0
        failed = ratio > rule.threshold if rule.threshold is not None else invalid_count > 0
        action = (rule.on_fail or "").lower() if rule.on_fail else None
        rule_metrics[rule.identifier] = {
            "invalid_rows": int(invalid_count),
            "ratio": ratio,
            "threshold": rule.threshold,
            "failed": bool(failed),
            "severity": rule.severity,
            "action": action,
        }

        if failed and action == "quarantine":
            quarantine_frames.append(
                enriched.filter(~F.col(column)).withColumn("_quarantine_reason", F.lit(rule.identifier))
            )

        should_reject = failed and rule.severity == "error"
        if should_reject:
            reject_reasons.append((column, rule.identifier))
        else:
            enriched = enriched.withColumn(column, F.lit(True))

    validity_expr = F.lit(True)
    for _, column in runtime:
        validity_expr = validity_expr & F.col(column)
    enriched = enriched.withColumn("__dc_valid", validity_expr)

    if reject_reasons:
        reasons = [F.when(~F.col(col), F.lit(reason)) for col, reason in reject_reasons]
        enriched = enriched.withColumn("__dc_reasons", F.array(*reasons))
        cleaned = F.array_distinct(F.expr("filter(__dc_reasons, x -> x is not null)"))
        enriched = enriched.withColumn(
            "_reject_reason",
            F.when(F.size(cleaned) > 0, F.array_join(cleaned, "; ")).otherwise(F.lit("")),
        )
    else:
        enriched = enriched.withColumn("_reject_reason", F.lit(""))

    invalid_rows = enriched.filter(~F.col("__dc_valid")).count()
    valid_rows = input_rows - invalid_rows

    if quarantine_frames:
        quarantine_df = quarantine_frames[0]
        for frame in quarantine_frames[1:]:
            quarantine_df = quarantine_df.unionByName(frame)
    else:
        quarantine_df = enriched.limit(0)

    drop_columns = [col for _, col in runtime]
    valid_df = enriched.filter(F.col("__dc_valid")).drop(*drop_columns, "__dc_valid", "__dc_reasons")
    invalid_df = enriched.filter(~F.col("__dc_valid")).drop(*drop_columns, "__dc_valid", "__dc_reasons")
    quarantine_df = quarantine_df.drop(*drop_columns, "__dc_valid", "__dc_reasons")

    metrics = {
        "input_rows": input_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "rules": rule_metrics,
    }

    LOGGER.info("Validación completada. métricas=%s", metrics)
    return ValidationResult(
        valid_df=valid_df,
        invalid_df=invalid_df,
        quarantine_df=quarantine_df,
        metrics=metrics,
        extras=extras,
    )
