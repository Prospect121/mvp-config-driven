"""Operaciones Delta Lakehouse para Microsoft Fabric."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict


LOGGER = logging.getLogger(__name__)


def _spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def write_delta(env, df, table_uri: str, options: Dict[str, Any]) -> None:
    """Escribe un DataFrame en Delta y ejecuta mantenimiento opcional."""

    spark = _spark()
    start = perf_counter()
    writer = df.write.format("delta").mode(options.get("mode", "append"))
    extra = {
        k: str(v)
        for k, v in (options or {}).items()
        if k not in {"mode", "optimize", "vacuum_hours", "zorder"}
    }
    if extra:
        writer = writer.options(**extra)
    writer.save(table_uri)
    duration = perf_counter() - start
    LOGGER.info(
        "Escritura Delta completada",
        extra={
            "action": "write",
            "table_uri": table_uri,
            "duration_seconds": round(duration, 3),
            "workspace_id": getattr(env, "workspace_id", None),
        },
    )

    if options.get("optimize") or options.get("zorder"):
        optimize_delta(
            env,
            table_uri,
            {"optimize": bool(options.get("optimize")), "zorder": options.get("zorder")},
        )
    if options.get("vacuum_hours"):
        optimize_delta(env, table_uri, {"vacuum_hours": options.get("vacuum_hours")})


def _run_sql(statement: str) -> None:
    spark = _spark()
    spark.sql(statement)


def optimize_delta(env, table_uri: str, options: Dict[str, Any]) -> None:
    """Ejecuta operaciones OPTIMIZE/VACUUM/ZORDER según corresponda."""

    statements: list[str] = []
    zorder_cols = options.get("zorder")
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        statements.append(f"OPTIMIZE delta.`{table_uri}` ZORDER BY ({cols})")
    elif options.get("optimize"):
        statements.append(f"OPTIMIZE delta.`{table_uri}`")
    if options.get("vacuum_hours"):
        hours = int(options["vacuum_hours"])
        statements.append(f"VACUUM delta.`{table_uri}` RETAIN {hours} HOURS")
    if not statements:
        return
    for stmt in statements:
        start = perf_counter()
        _run_sql(stmt)
        LOGGER.info(
            "Delta maintenance ejecutado",
            extra={
                "action": "maintenance",
                "statement": stmt.split(" ")[0].lower(),
                "table_uri": table_uri,
                "duration_seconds": round(perf_counter() - start, 3),
                "workspace_id": getattr(env, "workspace_id", None),
            },
        )
