"""Expectation-based data-quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping

try:  # Optional dependency - pyspark may not be available
    from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql import Column  # type: ignore
except Exception:  # pragma: no cover - pyspark optional
    SparkDataFrame = None  # type: ignore
    F = None  # type: ignore
    Column = Any  # type: ignore

try:  # Optional dependency
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - pandas optional
    pd = None  # type: ignore

try:  # Optional dependency
    import polars as pl  # type: ignore
except Exception:  # pragma: no cover - polars optional
    pl = None  # type: ignore


@dataclass
class ExpectationResult:
    """Outcome of a single expectation evaluation."""

    expectation: Dict[str, Any]
    failures: int

    @property
    def passed(self) -> bool:
        return self.failures == 0


def _is_spark(df: Any) -> bool:
    return SparkDataFrame is not None and isinstance(df, SparkDataFrame)


def _spark_filter(df: Any, condition: Column) -> int:
    if not hasattr(df, "filter"):
        raise TypeError("Spark DataFrame expected for spark expectations")
    return int(df.filter(condition).count())


def _evaluate_spark(df: Any, expectation: Mapping[str, Any]) -> int:
    if F is None:
        raise RuntimeError("pyspark.sql.functions not available for expectation evaluation")

    expectation_type = str(expectation.get("type") or "").lower()
    column = expectation.get("column")

    if expectation_type in {"condition"}:
        expr = expectation.get("expr") or expectation.get("expression")
        if not expr:
            raise ValueError("Condition expectation requires 'expr'")
        failures = df.filter(~F.expr(str(expr))).count()
        return int(failures)

    if not column:
        raise ValueError(f"Expectation '{expectation_type}' requires a 'column'")

    col = F.col(str(column))

    if expectation_type == "not_null":
        return _spark_filter(df, col.isNull())
    if expectation_type == "unique":
        duplicates = (
            df.groupBy(col).count().filter(F.col("count") > 1).select(F.sum("count") - 1)
        )
        row = duplicates.collect()
        if not row:
            return 0
        value = row[0][0]
        return int(value or 0)
    if expectation_type == "non_negative":
        return _spark_filter(df, col < 0)
    if expectation_type == "valid_values":
        values = expectation.get("values") or expectation.get("allowed_values")
        if not values:
            raise ValueError("valid_values expectation requires 'values'")
        return _spark_filter(df, ~col.isin([str(v) for v in values]))
    if expectation_type == "range":
        minimum = expectation.get("min")
        maximum = expectation.get("max")
        condition = None
        if minimum is not None:
            condition = (col < minimum) if condition is None else (condition | (col < minimum))
        if maximum is not None:
            condition = (col > maximum) if condition is None else (condition | (col > maximum))
        if condition is None:
            raise ValueError("range expectation requires 'min' or 'max'")
        return _spark_filter(df, condition)

    raise ValueError(f"Unsupported expectation type: {expectation_type}")


def _evaluate_pandas(df: Any, expectation: Mapping[str, Any]) -> int:
    expectation_type = str(expectation.get("type") or "").lower()
    column = expectation.get("column")

    if expectation_type == "condition":
        expr = expectation.get("expr") or expectation.get("expression")
        if not expr:
            raise ValueError("Condition expectation requires 'expr'")
        if pd is None:
            raise RuntimeError("pandas is required for condition expectations without Spark")
        if isinstance(df, pd.DataFrame):
            return int(df.query(f"not ({expr})").shape[0])
        if hasattr(df, "toPandas"):
            pdf = df.toPandas()  # type: ignore[no-untyped-call]
            return int(pdf.query(f"not ({expr})").shape[0])
        if pl is not None and isinstance(df, pl.DataFrame):  # pragma: no cover - optional
            pdf = df.to_pandas()
            return int(pdf.query(f"not ({expr})").shape[0])
        raise TypeError("Unsupported dataframe type for condition expectation")

    if not column:
        raise ValueError(f"Expectation '{expectation_type}' requires a 'column'")

    if pd is None:
        raise RuntimeError("pandas is required for expectation evaluation without Spark")

    if isinstance(df, pd.DataFrame):
        pdf = df
    elif hasattr(df, "toPandas"):
        pdf = df.toPandas()  # type: ignore[no-untyped-call]
    elif pl is not None and isinstance(df, pl.DataFrame):  # pragma: no cover - optional
        pdf = df.to_pandas()
    else:
        raise TypeError("Unsupported dataframe type for expectation evaluation")

    if column not in pdf.columns:
        return 0

    series = pdf[column]

    if expectation_type == "not_null":
        return int(series.isna().sum())
    if expectation_type == "unique":
        duplicates = series.duplicated(keep=False)
        return int(duplicates.sum())
    if expectation_type == "non_negative":
        return int((series < 0).sum())
    if expectation_type == "valid_values":
        values = expectation.get("values") or expectation.get("allowed_values")
        if not values:
            raise ValueError("valid_values expectation requires 'values'")
        mask = ~series.isin(values)
        return int(mask.sum())
    if expectation_type == "range":
        minimum = expectation.get("min")
        maximum = expectation.get("max")
        mask = pd.Series(False, index=series.index)
        if minimum is not None:
            mask |= series < minimum
        if maximum is not None:
            mask |= series > maximum
        return int(mask.sum())

    raise ValueError(f"Unsupported expectation type: {expectation_type}")


def apply_expectations(df: Any, expectations: Iterable[Mapping[str, Any]]) -> List[ExpectationResult]:
    """Evaluate expectations and raise on failures."""

    results: List[ExpectationResult] = []
    expectations = list(expectations or [])

    for expectation in expectations:
        if not expectation:
            continue
        if _is_spark(df):
            failures = _evaluate_spark(df, expectation)
        else:
            failures = _evaluate_pandas(df, expectation)
        result = ExpectationResult(expectation=dict(expectation), failures=int(failures))
        results.append(result)
        if result.failures:
            raise ValueError(
                "Expectation failed for type '{type}' on column '{column}'".format(
                    type=expectation.get("type"), column=expectation.get("column")
                )
            )

    return results


__all__ = ["ExpectationResult", "apply_expectations"]
