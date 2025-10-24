from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

import yaml

try:
    from pyspark.sql import DataFrame, SparkSession, functions as F
    from pyspark.sql.window import Window
except ModuleNotFoundError:  # pragma: no cover - pyspark optional for smoke tests
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore

    class _MissingFunctions:
        def __getattr__(self, name: str) -> Any:
            raise RuntimeError("pyspark is required for pipeline execution")

    class _MissingWindow:
        @staticmethod
        def partitionBy(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("pyspark is required for pipeline execution")

    F = _MissingFunctions()  # type: ignore
    Window = _MissingWindow()  # type: ignore
from pipelines.common import parse_order, safe_cast, maybe_config_s3a
from pipelines.sources import load_sources_or_source, flatten_json, sanitize_nulls, project_columns
from pipelines.udf_catalog import get_udf
from pipelines.validation.quality import apply_quality as apply_quality_mod
from pipelines.transforms.apply import (
    apply_sql_transforms as apply_sql_transforms_mod,
    apply_udf_transforms as apply_udf_transforms_mod,
)


def load_json_schema(path: str) -> Dict[str, Any]:
    """Load schema from JSON or YAML file."""
    _, ext = os.path.splitext(path.lower())
    with open(path, "r", encoding="utf-8") as handle:
        if ext in (".yml", ".yaml"):
            return yaml.safe_load(handle)
        try:
            return json.load(handle)
        except Exception:
            handle.seek(0)
            return yaml.safe_load(handle)


def spark_type_from_json(json_type: str) -> str:
    return {
        "string": "string",
        "number": "double",
        "integer": "int",
        "boolean": "boolean",
        "array": "array<string>",
        "object": "struct<>",
    }.get(json_type, "string")


def enforce_schema(df: DataFrame, json_schema: Dict[str, Any], mode: str = "strict") -> DataFrame:
    props = json_schema.get("properties", {})
    required = set(json_schema.get("required", []))

    for column, spec in props.items():
        json_type = spec.get("type")
        if isinstance(json_type, list):
            json_type = next((t for t in json_type if t != "null"), "string")

        spark_type = spark_type_from_json(json_type)

        if column in df.columns:
            if json_type == "string" and spec.get("format") == "date-time":
                df = df.withColumn(column, F.to_timestamp(F.col(column)))
            else:
                df = df.withColumn(column, F.col(column).cast(spark_type))
        elif column in required and mode == "strict":
            raise ValueError(f"Columna requerida '{column}' no encontrada")
        elif column in required:
            df = df.withColumn(column, F.lit(None).cast(spark_type))

    return df


def load_expectations(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_transforms(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except Exception as exc:
        print(f"[transforms] Warning: No se pudo cargar '{path}': {exc}")
        return {}


def apply_sql_transforms(df: DataFrame, transforms_cfg: Dict[str, Any]) -> DataFrame:
    if not transforms_cfg:
        return df

    transforms_list = (
        transforms_cfg.get("transforms")
        or transforms_cfg.get("sql")
        or []
    )
    if not transforms_list:
        return df

    default_on_error = (transforms_cfg.get("on_error") or "").lower() or None

    for transform in transforms_list:
        if not isinstance(transform, dict):
            print(f"[transforms] Ignorando entrada no soportada: {transform}")
            continue

        target = transform.get("target_column") or transform.get("name")
        expr = transform.get("expr")
        mode = (transform.get("mode") or "create").lower()
        cast_type = transform.get("type")
        on_error = transform.get("on_error") or default_on_error

        if not target or not expr:
            print(f"[transforms] Entrada inválida, falta target/expr: {transform}")
            continue

        try:
            target_exists = target in df.columns
            if mode == "create" and target_exists:
                print(f"[transforms] Skipped create for existing column '{target}'")
                continue

            df = df.withColumn(target, F.expr(expr))
            if cast_type:
                df = safe_cast(df, target, cast_type, on_error=on_error)
            print(f"[transforms] Applied expr to '{target}': {expr} type={cast_type or 'auto'}")
        except Exception as exc:
            message = f"[transforms] Error applying expr to '{target}': {exc}"
            if (on_error or '').lower() == 'null':
                target_type = cast_type or 'string'
                df = df.withColumn(target, F.lit(None).cast(target_type))
                print(message + f" -> set NULL ({target_type})")
            elif (on_error or '').lower() == 'skip':
                print(message + " -> skipped")
            else:
                print(message)
                raise

    return df


def apply_udf_transforms(df: DataFrame, transforms_cfg: Dict[str, Any]) -> DataFrame:
    if not transforms_cfg:
        return df

    udf_list = transforms_cfg.get("udf") or []
    if not udf_list:
        return df

    default_on_error = (transforms_cfg.get("on_error") or "").lower() or None

    for transform in udf_list:
        if not isinstance(transform, dict):
            print(f"[udf] Ignorando entrada no soportada: {transform}")
            continue

        target = transform.get("target_column") or transform.get("name")
        func_name = transform.get("function")
        args = transform.get("args", [])
        mode = (transform.get("mode") or "create").lower()
        cast_type = transform.get("type")
        on_error = transform.get("on_error") or default_on_error

        if not target or not func_name:
            print(f"[udf] Entrada inválida, falta target/function: {transform}")
            continue

        udf_func = get_udf(func_name)
        if not udf_func:
            print(f"[udf] UDF no encontrada en catálogo: {func_name}")
            continue

        try:
            target_exists = target in df.columns
            if mode == "create" and target_exists:
                print(f"[udf] Skipped create for existing column '{target}'")
                continue

            bound_args = []
            for arg in args:
                if isinstance(arg, str) and arg in df.columns:
                    bound_args.append(F.col(arg))
                else:
                    bound_args.append(F.lit(arg))

            df = df.withColumn(target, udf_func(*bound_args))
            if cast_type:
                df = safe_cast(df, target, cast_type, on_error=on_error)
            print(
                f"[udf] Applied UDF '{func_name}' to '{target}' args={args} type={cast_type or 'auto'}"
            )
        except Exception as exc:
            message = f"[udf] Error applying UDF '{func_name}' to '{target}': {exc}"
            if (on_error or '').lower() == 'null':
                target_type = cast_type or 'string'
                df = df.withColumn(target, F.lit(None).cast(target_type))
                print(message + f" -> set NULL ({target_type})")
            elif (on_error or '').lower() == 'skip':
                print(message + " -> skipped")
            else:
                print(message)
                raise

    return df


def apply_quality(df: DataFrame, rules: Iterable[Dict[str, Any]], quarantine_path: Optional[str], run_id: str):
    return apply_quality_mod(df, list(rules or []), quarantine_path, run_id)


def run_raw_sources(cfg: Dict[str, Any], spark: SparkSession, env: Dict[str, Any]) -> DataFrame:
    maybe_config_s3a(spark, cfg["output"]["silver"]["path"], env)
    df = load_sources_or_source(cfg, spark, env)
    sources_list = cfg.get("sources")
    if sources_list:
        print(f"[source] Loaded {df.count()} rows from {len(sources_list)} sources")
    else:
        src = cfg.get("source", {})
        src_desc = (
            src.get("path")
            or src.get("jdbc", {}).get("table")
            or src.get("api", {}).get("endpoint")
            or "<source>"
        )
        print(f"[source] Loaded {df.count()} rows from {src_desc}")
    return df


def run_bronze_stage(cfg: Dict[str, Any], env: Dict[str, Any], spark: SparkSession, df: DataFrame) -> DataFrame:
    bronze_cfg = (cfg.get("output", {}).get("bronze") or cfg.get("bronze") or {})
    if not bronze_cfg.get("enabled", False):
        return df

    bronze_path = bronze_cfg.get("path")
    bronze_format = (bronze_cfg.get("format") or "parquet").lower()
    bronze_mode = (bronze_cfg.get("mode") or "overwrite").lower()
    compression = bronze_cfg.get("compression", "snappy")
    part_cols = bronze_cfg.get("partition_by", []) or []
    base_col_name = bronze_cfg.get("partition_from")
    base_col = F.col(base_col_name) if base_col_name and base_col_name in df.columns else None

    if not bronze_path:
        return df

    maybe_config_s3a(spark, bronze_path, env)
    writer = df.write.mode(bronze_mode).option("compression", compression)

    if part_cols:
        if base_col is not None:
            ts_col = F.to_timestamp(base_col)
            for col in part_cols:
                if col not in df.columns:
                    lowered = col.lower()
                    if lowered == "year":
                        df = df.withColumn("year", F.year(ts_col))
                    elif lowered == "month":
                        df = df.withColumn("month", F.month(ts_col))
                    elif lowered == "day":
                        df = df.withColumn("day", F.dayofmonth(ts_col))
                    elif lowered == "date":
                        df = df.withColumn("date", F.to_date(ts_col))
            writer = writer.partitionBy(*part_cols)
        else:
            existing = [col for col in part_cols if col in df.columns]
            if len(existing) == len(part_cols):
                writer = writer.partitionBy(*part_cols)
            else:
                print(f"[bronze] Base column '{base_col_name}' not found; skipping partitioning")

    if bronze_format != "parquet":
        print(f"[bronze] Formato '{bronze_format}' no soportado, usando parquet")
    writer.parquet(bronze_path)
    df = spark.read.parquet(bronze_path)
    print(f"[bronze] Wrote and reloaded Bronze dataset at {bronze_path}")
    return df


def run_silver_stage(cfg: Dict[str, Any], env: Dict[str, Any], spark: SparkSession, df: DataFrame, db_manager) -> DataFrame:
    std = cfg.get("standardization", {})
    json_norm = cfg.get("json_normalization", {})
    should_flatten = json_norm.get("flatten", True)
    is_jsonish = False

    sources_list = cfg.get("sources")
    src = cfg.get("source", {})
    if sources_list:
        for source in sources_list:
            fmt = (source.get("input_format") or "").lower()
            if fmt in ("json", "jsonl") or source.get("type") == "api":
                is_jsonish = True
                break
    else:
        fmt = (src.get("input_format") or "").lower()
        if fmt in ("json", "jsonl") or src.get("type") == "api":
            is_jsonish = True

    if should_flatten and is_jsonish:
        df = flatten_json(df, json_norm.get("paths"))

    nulls_cfg = cfg.get("null_handling", {})
    if nulls_cfg:
        df = sanitize_nulls(df, fills=nulls_cfg.get("fills"), drop_if_null=nulls_cfg.get("drop_if_null"))

    for rename in std.get("rename", []) or []:
        from_col = rename["from"]
        to_col = rename["to"]
        if "." in from_col:
            df = df.withColumn(to_col, F.col(from_col))
        elif from_col in df.columns:
            df = df.withColumnRenamed(from_col, to_col)

    keep_cols = cfg.get("select_columns")
    if keep_cols:
        df = project_columns(df, keep_cols)

    if "schema" in cfg and cfg["schema"].get("ref"):
        schema_cfg = cfg["schema"]
        json_schema = load_json_schema(schema_cfg["ref"])
        mode = schema_cfg.get("mode", "strict").lower()
        df = enforce_schema(df, json_schema, mode=mode)

    for cast in std.get("casts", []) or []:
        df = safe_cast(
            df,
            cast["column"],
            cast["to"],
            format_hint=cast.get("format_hint"),
            on_error=cast.get("on_error", "fail"),
        )

    for default in std.get("defaults", []) or []:
        column = default["column"]
        value = default["value"]
        if column in df.columns:
            df = df.withColumn(
                column,
                F.when(F.col(column).isNull(), F.lit(value)).otherwise(F.col(column)),
            )
        else:
            df = df.withColumn(column, F.lit(value))

    if "deduplicate" in std:
        dedup_cfg = std["deduplicate"]
        key = dedup_cfg["key"]
        order = parse_order(dedup_cfg.get("order_by", []))
        window = Window.partitionBy(*key).orderBy(*order) if order else Window.partitionBy(*key)
        df = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    if cfg.get("transforms_ref"):
        transforms_path = cfg["transforms_ref"]
        try:
            transforms_cfg = load_transforms(transforms_path)
            if transforms_cfg:
                df = apply_sql_transforms_mod(df, transforms_cfg)
                df = apply_udf_transforms_mod(df, transforms_cfg)
            else:
                print(f"[transforms] No transforms found in {transforms_path}")
        except Exception as exc:
            print(f"[transforms] Error applying transforms: {exc}")

    out = cfg["output"]["silver"]
    parts = out.get("partition_by", [])
    if parts:
        base_col_name = out.get("partition_from")
        base_col = F.col(base_col_name) if base_col_name and base_col_name in df.columns else None
        if base_col is not None:
            for part in parts:
                if part not in df.columns:
                    lowered = part.lower()
                    if lowered == "year":
                        df = df.withColumn("year", F.year(base_col))
                    elif lowered == "month":
                        df = df.withColumn("month", F.month(base_col))
                    elif lowered == "day":
                        df = df.withColumn("day", F.dayofmonth(base_col))
                    elif lowered == "date":
                        df = df.withColumn("date", F.to_date(base_col))

    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
    df = df.withColumn("_run_id", F.lit(run_id)).withColumn("_ingestion_ts", F.current_timestamp())

    if "quality" in cfg and cfg["quality"].get("expectations_ref"):
        quality_cfg = cfg["quality"]
        expectations = load_expectations(quality_cfg["expectations_ref"]) or {}
        rules = expectations.get("rules", [])
        quarantine_path = quality_cfg.get("quarantine")
        maybe_config_s3a(spark, quarantine_path or "", env)
        df, _, stats = apply_quality_mod(df, rules, quarantine_path, run_id)
        print("[quality] stats:", stats)

    df.printSchema()
    final_count = df.count()
    print(f"[silver] Final row count: {final_count}")

    writer = (
        df.write.format(out.get("format", "parquet"))
        .option("mergeSchema", str(out.get("merge_schema", True)).lower())
    )

    if parts:
        writer = writer.partitionBy(*parts)

    mode_cfg = (out.get("mode", "append") or "append").lower()
    if mode_cfg == "overwrite_dynamic":
        writer.mode("overwrite").save(out["path"])
    else:
        writer.mode("append").save(out["path"])

    print(f"[silver] Successfully wrote to {out['path']}")

    if db_manager:
        try:
            silver_version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            db_manager.log_dataset_version(
                dataset_name=f"{cfg['id']}_silver",
                version=silver_version,
                schema_path=cfg.get("schema", {}).get("ref"),
                record_count=final_count,
            )
            print(f"[metadata] Silver dataset version logged: {silver_version}")
        except Exception as exc:
            print(f"[metadata] Warning: Failed to log silver dataset version: {exc}")

    return df


def create_gold_table_name(dataset_id: str, table_settings: Dict[str, Any]) -> str:
    prefix = table_settings.get("table_prefix", "")
    suffix = table_settings.get("table_suffix", "")
    return f"{prefix}{dataset_id}{suffix}".strip("_")


def apply_business_rules(df: DataFrame, rules: Iterable[Dict[str, Any]]) -> DataFrame:
    if not rules:
        return df

    for rule in rules:
        if rule.get("action") == "filter":
            condition = rule.get("condition")
            if condition:
                initial = df.count()
                df = df.filter(F.expr(condition))
                final = df.count()
                print(f"[gold] Applied business rule '{condition}': {initial} -> {final} rows")
            else:
                print(f"[gold] Warning: Business rule missing condition: {rule}")
    return df


def apply_gold_transformations(df: DataFrame, gold_config: Dict[str, Any], table_settings: Dict[str, Any]) -> DataFrame:
    default_transforms = table_settings.get("default_transformations", {})
    exclude_columns = gold_config.get("exclude_columns", default_transforms.get("exclude_columns", []))
    if exclude_columns:
        existing = [col for col in exclude_columns if col in df.columns]
        if existing:
            df = df.drop(*existing)
            print(f"[gold] Excluded columns: {existing}")

    add_cols = gold_config.get("add_columns") or default_transforms.get("add_columns", [])
    for definition in add_cols:
        if isinstance(definition, dict) and "name" in definition and "value" in definition:
            col_name = definition["name"]
            col_value = definition["value"]
            col_type = definition.get("type", "string")

            if col_value == "current_timestamp()":
                df = df.withColumn(col_name, F.current_timestamp())
            elif col_value == "current_date()":
                df = df.withColumn(col_name, F.current_date())
            elif isinstance(col_value, str) and col_value.startswith("uuid()"):
                df = df.withColumn(col_name, F.monotonically_increasing_id().cast("string"))
            else:
                df = df.withColumn(col_name, F.lit(col_value).cast(col_type))

            print(f"[gold] Added column '{col_name}' with value '{col_value}' as {col_type}")

    business_rules = gold_config.get("business_rules") or default_transforms.get("business_rules", [])
    df = apply_business_rules(df, business_rules)
    return df


def write_to_gold_database(
    df: DataFrame,
    dataset_id: str,
    schema_path: str,
    db_manager,
    table_settings: Dict[str, Any],
) -> bool:
    try:
        table_name = create_gold_table_name(dataset_id, table_settings)
        schema_version = datetime.now().strftime(table_settings.get("version_format", "%Y%m%d_%H%M%S"))
        print(f"[gold] Creating/updating table '{table_name}' from schema '{schema_path}'")

        schema_dict = load_json_schema(schema_path)
        success = db_manager.create_table_from_schema(
            table_name=table_name,
            schema_dict=schema_dict,
            schema_version=schema_version,
        )

        if not success:
            raise Exception(f"Failed to create/update table schema for {table_name}")

        json_schema = load_json_schema(schema_path)
        expected_columns = list(json_schema.get("properties", {}).keys()) if "properties" in json_schema else []
        available_columns = [col for col in expected_columns if col in df.columns]
        if available_columns:
            df_filtered = df.select(*available_columns)
            print(f"[gold] Filtered DataFrame to schema columns: {available_columns}")
        else:
            df_filtered = df
            print(f"[gold] Warning: No schema columns found, using full DataFrame")

        write_mode = table_settings.get("default_write_mode", "append")
        upsert_keys = table_settings.get("upsert_keys")
        success = db_manager.write_dataframe(df_filtered, table_name, write_mode, upsert_keys)

        if success:
            print(f"[gold] Successfully wrote data to table '{table_name}' in {write_mode} mode")
        else:
            raise Exception(f"Failed to write data to table {table_name}")

        return True
    except Exception as exc:
        print(f"[gold] Error writing to database: {exc}")
        return False


def write_to_gold_bucket(
    df: DataFrame,
    dataset_id: str,
    schema_path: Optional[str],
    bucket_cfg: Dict[str, Any],
    env: Dict[str, Any],
    spark: SparkSession,
) -> bool:
    try:
        path = bucket_cfg.get("path")
        if not path:
            raise ValueError("Gold bucket configuration requires 'path'")

        fmt = (bucket_cfg.get("format") or "parquet").lower()
        mode = (bucket_cfg.get("mode") or "append").lower()
        compression = bucket_cfg.get("compression", "snappy")
        merge_schema = str(bucket_cfg.get("merge_schema", True)).lower()
        partition_by = bucket_cfg.get("partition_by", []) or []
        partition_from = bucket_cfg.get("partition_from")
        coalesce_n = bucket_cfg.get("coalesce")
        repartition = bucket_cfg.get("repartition")

        maybe_config_s3a(spark, path, env)

        base_col = F.col(partition_from) if partition_from and partition_from in df.columns else None
        if base_col is not None and partition_by:
            ts_col = F.to_timestamp(base_col)
            for part in partition_by:
                if part not in df.columns:
                    lowered = part.lower()
                    if lowered == "year":
                        df = df.withColumn("year", F.year(ts_col))
                    elif lowered == "month":
                        df = df.withColumn("month", F.month(ts_col))
                    elif lowered == "day":
                        df = df.withColumn("day", F.dayofmonth(ts_col))
                    elif lowered == "date":
                        df = df.withColumn("date", F.to_date(ts_col))

        if isinstance(coalesce_n, int) and coalesce_n > 0:
            df = df.coalesce(coalesce_n)
        elif isinstance(repartition, int) and repartition > 0:
            df = df.repartition(repartition)
        elif isinstance(repartition, list) and repartition:
            cols = [col for col in repartition if col in df.columns]
            if cols:
                df = df.repartition(*[F.col(col) for col in cols])

        writer = df.write.format(fmt).mode("overwrite" if mode == "overwrite" else "append")
        writer = writer.option("compression", compression).option("mergeSchema", merge_schema)
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        if fmt in ("parquet", "json", "csv"):
            writer.save(path)
        else:
            print(f"[gold][bucket] Unsupported format '{fmt}', falling back to parquet")
            df.write.mode(mode).option("compression", compression).save(path)

        print(f"[gold][bucket] Wrote {df.count()} rows to {path}")
        return True
    except Exception as exc:
        print(f"[gold][bucket] Error writing to bucket: {exc}")
        return False
