"""Lógica incremental."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


def merge_delta(target_path: str, df: DataFrame, keys: list[str]) -> None:
    try:
        from delta.tables import DeltaTable  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Delta Lake no disponible para merges") from exc

    spark = df.sparkSession
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(target_path)


def _path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def merge_generic(spark: SparkSession, df: DataFrame, target_path: str, keys: list[str]) -> None:
    if not _path_exists(spark, target_path):
        df.write.mode("overwrite").parquet(target_path)
        return
    existing = spark.read.parquet(target_path)
    combined = existing.unionByName(df, allowMissingColumns=True)

    window = Window.partitionBy(*keys).orderBy("_ingestion_ts")
    deduped = combined.withColumn("_rn", row_number().over(window)).filter("_rn = 1").drop("_rn")
    deduped.write.mode("overwrite").parquet(target_path)


def handle_incremental(df: DataFrame, sink: dict[str, Any], mode: str, keys: list[str]) -> None:
    if mode == "append":
        df.write.mode("append").format(sink.get("format", "parquet")).save(sink["uri"])
        return
    if mode == "merge":
        if sink.get("format") == "delta":
            merge_delta(sink["uri"], df, keys)
        else:
            merge_generic(df.sparkSession, df, sink["uri"], keys)
        return
    raise ValueError(f"Modo incremental no soportado: {mode}")
