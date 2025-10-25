"""JDBC ingestion utilities with partitioning and incremental support."""

from __future__ import annotations

import os
from typing import Any, Dict, Mapping, Optional, Tuple

try:  # Optional dependency - pyspark may not be installed during unit tests
    from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore
    from pyspark.sql import functions as F  # type: ignore
except Exception:  # pragma: no cover - pyspark optional
    SparkDataFrame = None  # type: ignore
    F = None  # type: ignore


class JDBCConfigurationError(ValueError):
    """Raised when the JDBC configuration is invalid."""


def _env_value(name: Any) -> Optional[str]:
    if not name:
        return None
    value = os.getenv(str(name))
    if value is None:
        return None
    value = value.strip()
    return value or None


def _resolve_auth_properties(cfg: Mapping[str, Any]) -> Dict[str, str]:
    auth_cfg = cfg.get("auth") or {}
    props: Dict[str, str] = {}

    mode = (auth_cfg.get("mode") or auth_cfg.get("type") or "").lower()
    if mode in {"managed_identity", "identity"} or auth_cfg.get("use_managed_identity"):
        props["azure.identity.auth.type"] = "ManagedIdentity"
        return props

    username = auth_cfg.get("user") or auth_cfg.get("username")
    password = auth_cfg.get("password")

    if not username:
        username = _env_value(auth_cfg.get("user_env") or auth_cfg.get("username_env"))
    if not password:
        password = _env_value(auth_cfg.get("password_env"))

    if mode in {"basic_env", "env"}:
        username = _env_value(auth_cfg.get("username_env") or auth_cfg.get("user_env"))
        password = _env_value(auth_cfg.get("password_env"))

    if username:
        props["user"] = username
    if password:
        props["password"] = password

    if not props and mode and mode not in {"none", "managed_identity", "identity"}:
        raise JDBCConfigurationError("Failed to resolve JDBC authentication credentials")

    return props


def _ensure_tls(url: str, options: Dict[str, Any]) -> None:
    lowered = url.lower()
    if lowered.startswith("jdbc:postgresql:"):
        options.setdefault("ssl", "true")
    elif lowered.startswith("jdbc:mysql:"):
        options.setdefault("useSSL", "true")
    elif lowered.startswith("jdbc:sqlserver:"):
        options.setdefault("encrypt", "true")
    elif lowered.startswith("jdbc:oracle:"):
        options.setdefault("oracle.net.tns_admin", options.get("oracle.net.tns_admin", ""))
    # Do not allow toggling TLS off
    for key in ("ssl", "useSSL", "encrypt"):
        value = str(options.get(key, "true")).lower()
        if value in {"false", "0", "no"}:
            raise JDBCConfigurationError("TLS must remain enabled for JDBC connections")


def _render_query(cfg: Mapping[str, Any]) -> str:
    query = cfg.get("query")
    table = cfg.get("table")
    if query:
        rendered = str(query)
    elif table:
        rendered = f"SELECT * FROM {table}"
    else:
        raise JDBCConfigurationError("JDBC source requires 'query' or 'table'")

    incremental_cfg = cfg.get("incremental") or {}
    watermark_cfg = incremental_cfg.get("watermark") or {}
    watermark_value = watermark_cfg.get("value") or watermark_cfg.get("resolved_value")
    placeholder = watermark_cfg.get("placeholder") or "${RAW_WM_TS}"

    if watermark_value is not None and placeholder in rendered:
        rendered = rendered.replace(placeholder, str(watermark_value))

    return rendered


def _discover_partition_bounds(
    spark: Any,
    cfg: Mapping[str, Any],
    query: str,
    column: str,
    options: Mapping[str, Any],
    props: Mapping[str, Any],
) -> Tuple[Any, Any]:
    if spark is None:
        raise JDBCConfigurationError(
            "Automatic partition discovery requires a Spark session"
        )

    subquery_alias = cfg.get("partitioning", {}).get("alias") or "base"
    bound_query = (
        f"SELECT MIN({column}) AS lower_bound, MAX({column}) AS upper_bound "
        f"FROM ({query}) AS {subquery_alias}"
    )

    reader = spark.read.format("jdbc").option("url", str(cfg.get("url")))
    reader = reader.option("driver", str(cfg.get("driver")))
    reader = reader.option("query", bound_query)

    for key, value in options.items():
        reader = reader.option(str(key), value)
    for key, value in props.items():
        reader = reader.option(str(key), value)

    bounds_df = reader.load()
    rows = bounds_df.collect()
    if not rows:
        raise JDBCConfigurationError("Unable to discover JDBC partition bounds")
    row = rows[0]
    return row["lower_bound"], row["upper_bound"]


def _determine_partitioning(
    spark: Any,
    cfg: Mapping[str, Any],
    query: str,
    options: Mapping[str, Any],
    props: Mapping[str, Any],
) -> Dict[str, Any]:
    partition_cfg = cfg.get("partitioning") or {}
    column = partition_cfg.get("column") or partition_cfg.get("field")
    if not column:
        return {}

    partitions = partition_cfg.get("num_partitions") or partition_cfg.get("partitions")
    if partitions is None:
        raise JDBCConfigurationError("JDBC partitioning requires 'num_partitions'")

    lower = partition_cfg.get("lower_bound")
    upper = partition_cfg.get("upper_bound")
    discover = partition_cfg.get("discover")
    if (lower is None or upper is None) and (discover is None or bool(discover)):
        lower, upper = _discover_partition_bounds(spark, cfg, query, str(column), options, props)

    if lower is None or upper is None:
        raise JDBCConfigurationError(
            "JDBC partitioning requires 'lower_bound' and 'upper_bound' or discovery enabled"
        )

    return {
        "partitionColumn": str(column),
        "lowerBound": lower if isinstance(lower, (int, float)) else str(lower),
        "upperBound": upper if isinstance(upper, (int, float)) else str(upper),
        "numPartitions": int(partitions),
    }


def _compute_watermark(df: Any, field: str) -> Optional[str]:
    if SparkDataFrame is not None and isinstance(df, SparkDataFrame):  # pragma: no cover - heavy
        if F is None:
            return None
        row = df.select(F.max(F.col(field)).alias("wm")).collect()
        if row:
            value = row[0]["wm"]
            return None if value is None else str(value)
        return None
    if hasattr(df, "toPandas"):
        pdf = df.toPandas()  # type: ignore[no-untyped-call]
        if field in pdf.columns and not pdf.empty:
            value = pdf[field].max()
            return None if value is None else str(value)
    return None


def read_jdbc(cfg: Mapping[str, Any], spark: Any) -> Tuple[Any, Optional[str]]:
    """Load data from a JDBC source using Spark."""

    if spark is None:
        raise RuntimeError("JDBC ingestion requires a Spark session")

    if not isinstance(cfg, Mapping):
        raise TypeError("JDBC configuration must be a mapping")

    url = cfg.get("url")
    if not url:
        raise JDBCConfigurationError("JDBC source requires 'url'")

    driver = cfg.get("driver")
    if not driver:
        raise JDBCConfigurationError("JDBC source requires 'driver'")

    options: Dict[str, Any] = dict(cfg.get("options") or {})
    _ensure_tls(str(url), options)

    props = _resolve_auth_properties(cfg)
    query = _render_query(cfg)

    reader = spark.read.format("jdbc").option("url", str(url)).option("driver", str(driver))
    reader = reader.option("query", query)

    for key, value in options.items():
        reader = reader.option(str(key), value)

    for key, value in props.items():
        reader = reader.option(str(key), value)

    partitioning = _determine_partitioning(spark, cfg, query, options, props)
    for key, value in partitioning.items():
        reader = reader.option(key, value)

    df = reader.load()

    watermark_field = ((cfg.get("incremental") or {}).get("watermark") or {}).get("field")
    watermark_value = None
    if watermark_field:
        watermark_value = _compute_watermark(df, str(watermark_field))

    return df, watermark_value


__all__ = ["read_jdbc", "JDBCConfigurationError"]
