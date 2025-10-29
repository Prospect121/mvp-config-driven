"""Validaciones de datasets."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)


def apply_validation(df: DataFrame, rules: dict[str, Any]) -> dict[str, int]:
    metrics = {"input_rows": df.count()}
    if not rules:
        metrics["valid_rows"] = metrics["input_rows"]
        metrics["invalid_rows"] = 0
        return metrics

    invalid_df = df
    if "expect_not_null" in rules:
        for column in rules["expect_not_null"]:
            invalid_df = invalid_df.filter(col(column).isNull())
    if "expect_unique" in rules:
        for column in rules["expect_unique"]:
            duplicates = df.groupBy(column).count().filter(col("count") > 1).count()
            metrics[f"duplicates_{column}"] = duplicates
    if "expect_domain" in rules:
        for column, allowed in rules["expect_domain"].items():
            invalid_df = invalid_df.filter(~col(column).isin(*allowed))
    invalid_count = invalid_df.count()
    metrics["invalid_rows"] = invalid_count
    metrics["valid_rows"] = metrics["input_rows"] - invalid_count
    LOGGER.info("Validación completada. métricas=%s", metrics)
    return metrics
