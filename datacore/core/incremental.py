"""Lógica incremental para storage y warehouses."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from datacore.connectors.db import jdbc
from datacore.platforms.base import PlatformBase


def merge_delta(target_path: str, df: DataFrame, keys: list[str]) -> None:
    try:
        from delta.tables import DeltaTable  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Delta Lake no disponible para merges") from exc

    spark = df.sparkSession
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        (
            delta_table.alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(target_path)


def _deduplicate(df: DataFrame, keys: list[str], order_by: list[str]) -> DataFrame:
    def _to_order(expr: str):
        parts = expr.strip().split()
        column = parts[0]
        direction = parts[1].lower() if len(parts) > 1 else "asc"
        col_expr = F.col(column)
        return col_expr.desc() if direction == "desc" else col_expr.asc()

    window = Window.partitionBy(*[F.col(key) for key in keys]).orderBy(*[_to_order(expr) for expr in order_by])
    ranked = df.withColumn("__dc_merge_rank", F.row_number().over(window))
    return ranked.filter(F.col("__dc_merge_rank") == 1).drop("__dc_merge_rank")


def _path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def merge_storage(
    spark: SparkSession,
    df: DataFrame,
    sink: dict[str, Any],
    keys: list[str],
    order_by: list[str],
) -> None:
    uri = sink["uri"]
    fmt = sink.get("format", "parquet")
    options = sink.get("options", {})
    if fmt == "delta":
        merge_delta(uri, df, keys)
        return

    if not _path_exists(spark, uri):
        df.write.format(fmt).mode("overwrite").options(**options).save(uri)
        return

    existing = spark.read.format(fmt).options(**options).load(uri).withColumn("__dc_is_new", F.lit(0))
    incoming = df.withColumn("__dc_is_new", F.lit(1))
    combined = existing.unionByName(incoming, allowMissingColumns=True)
    order_with_flag = [*order_by, "__dc_is_new DESC"]
    deduped = _deduplicate(combined, keys, order_with_flag).drop("__dc_is_new")
    deduped.write.format(fmt).mode("overwrite").options(**options).save(uri)


def merge_jdbc(df: DataFrame, sink: dict[str, Any], keys: list[str], order_by: list[str]) -> None:
    spark = df.sparkSession
    existing = jdbc.read(spark, sink)
    combined = existing.unionByName(df, allowMissingColumns=True)
    deduped = _deduplicate(combined, keys, order_by)
    overwrite_conf = {**sink, "mode": "overwrite", "truncate": True}
    jdbc.write(deduped, overwrite_conf)


def _merge_with_platform(
    platform: PlatformBase | None,
    df: DataFrame,
    sink: dict[str, Any],
    keys: list[str],
    order_by: list[str],
) -> bool:
    if platform and hasattr(platform, "merge_into_warehouse"):
        try:
            handled = platform.merge_into_warehouse(df, sink, keys, order_by)
            if handled:
                return True
        except NotImplementedError:
            pass
    return False


def prepare_incremental(
    df: DataFrame,
    sink: dict[str, Any],
    incremental_cfg: dict[str, Any] | None,
    platform: PlatformBase | None = None,
) -> tuple[DataFrame, dict[str, Any], bool]:
    if not incremental_cfg:
        return df, sink, False
    mode = str(incremental_cfg.get("mode", "append")).lower()
    if mode == "full":
        updated_sink = {**sink, "mode": "overwrite"}
        return df, updated_sink, False
    if mode == "append":
        return df, sink, False
    if mode != "merge":
        raise ValueError(f"Modo incremental no soportado: {mode}")

    keys = incremental_cfg.get("keys") or []
    if not keys:
        raise ValueError("Se requieren keys para modo merge")
    order_by = incremental_cfg.get("order_by", ["_ingestion_ts DESC"])

    if sink["type"] == "storage":
        merge_storage(df.sparkSession, df, sink, keys, order_by)
        return df, sink, True

    if sink["type"] == "warehouse":
        if _merge_with_platform(platform, df, sink, keys, order_by):
            return df, sink, True
        merge_jdbc(df, sink, keys, order_by)
        return df, sink, True

    if sink["type"] == "nosql" and platform and hasattr(platform, "merge_into_nosql"):
        handled = platform.merge_into_nosql(df, sink, keys, order_by)  # type: ignore[attr-defined]
        if handled:
            return df, sink, True

    raise ValueError(f"Merge incremental no soportado para sink {sink['type']}")
