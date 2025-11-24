"""Operaciones Delta Lakehouse para Microsoft Fabric."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict

from datacore.providers.fabric.uris import (
    parse_fabric_lakehouse_uri,
    resolve_fabric_files_path,
)


LOGGER = logging.getLogger(__name__)


def _spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def write_delta(env, df, table_uri: str, options: Dict[str, Any]) -> None:
    """Escribe un DataFrame en Delta y ejecuta mantenimiento opcional."""

    spark = _spark()
    start = perf_counter()
    opts: Dict[str, Any] = options or {}
    writer = df.write.format("delta")
    extra = {
        k: str(v)
        for k, v in opts.items()
        if k not in {"mode", "optimize", "vacuum_hours", "zorder"}
    }
    if extra:
        writer = writer.options(**extra)
    mode_value = str(opts.get("mode", "append"))
    normalized_mode = mode_value.lower()
    ref = parse_fabric_lakehouse_uri(table_uri)

    if ref.is_lakehouse and ref.kind == "tables" and ref.full_table_name:
        if normalized_mode == "overwrite":
            writer = writer.mode("overwrite").option("overwriteSchema", "true")
        else:
            writer = writer.mode(mode_value)
        LOGGER.info(
            "Fabric managed table write",
            extra={
                "event": "fabric_save_as_managed_table",
                "backend": "fabric",
                "lakehouse": ref.lakehouse,
                "schema": ref.schema,
                "table": ref.table,
                "mode": normalized_mode,
            },
        )
        writer.saveAsTable(ref.full_table_name)
    elif ref.is_lakehouse and ref.kind == "files" and ref.lakehouse:
        resolved = resolve_fabric_files_path(ref)
        LOGGER.info(
            "Fabric files write",
            extra={
                "event": "fabric_write_files_path",
                "backend": "fabric",
                "lakehouse": ref.lakehouse,
                "resolved_path": resolved,
                "mode": mode_value,
            },
        )
        writer = writer.mode(mode_value)
        writer.save(resolved)
    else:
        LOGGER.info(
            "Fabric raw path write",
            extra={
                "event": "fabric_raw_path_write",
                "backend": "fabric",
                "uri": table_uri,
                "mode": mode_value,
            },
        )
        writer = writer.mode(mode_value)
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
