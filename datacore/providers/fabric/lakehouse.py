"""Operaciones Delta Lakehouse para Microsoft Fabric."""

from __future__ import annotations

from typing import Any, Dict


def _spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def write_delta(env, df, table_uri: str, options: Dict[str, Any]) -> None:
    """Escribe un DataFrame en un destino Delta compatible con Fabric."""

    _spark()
    writer = df.write.format("delta").mode(options.get("mode", "append"))
    extra = {k: str(v) for k, v in (options or {}).items() if k != "mode"}
    if extra:
        writer = writer.options(**extra)
    writer.save(table_uri)


def optimize_delta(env, table_uri: str, options: Dict[str, Any]) -> None:
    """Ejecuta operaciones de optimización Delta (noop si no aplica)."""

    spark = _spark()
    if options.get("optimize"):
        spark.sql(f"OPTIMIZE delta.`{table_uri}`")
    if "vacuum_hours" in options:
        hours = options["vacuum_hours"]
        spark.sql(f"VACUUM delta.`{table_uri}` RETAIN {hours} HOURS")
