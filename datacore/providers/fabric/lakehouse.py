"""Operaciones Delta Lakehouse para Microsoft Fabric."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, Optional
from urllib.parse import urlparse


LOGGER = logging.getLogger(__name__)


def _spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def _fabric_table_from_uri(uri: str) -> tuple[bool, Optional[tuple[str, str]]]:
    """Return schema/table when the URI points to a Fabric Lakehouse table."""

    if not uri:
        return False, None

    parsed = urlparse(uri)
    segments: list[str] = []
    if parsed.netloc:
        segments.append(parsed.netloc)
    segments.extend(seg for seg in parsed.path.split("/") if seg)

    if not segments:
        return False, None

    tables_index: Optional[int] = next(
        (idx for idx, value in enumerate(segments) if value.lower() == "tables"),
        None,
    )
    if tables_index is None:
        return False, None

    remainder = segments[tables_index + 1 :]
    if not remainder:
        return True, None

    schema: Optional[str]
    table: Optional[str]
    if len(remainder) == 1:
        item = remainder[0]
        if "." in item:
            schema, table = item.split(".", 1)
        else:
            schema, table = "dbo", item
    else:
        schema = remainder[0] or "dbo"
        table = remainder[1] if len(remainder) > 1 else None

    if not table:
        return True, None

    return True, (schema or "dbo", table)


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
    is_tables_uri, table_identifier = _fabric_table_from_uri(table_uri)

    if table_identifier:
        schema, table = table_identifier
        LOGGER.info(
            "Fabric Lakehouse table sink detected",
            extra={
                "platform": "fabric",
                "event": "fabric_table_sink_detected",
                "schema": schema,
                "table": table,
                "uri": table_uri,
            },
        )
        if normalized_mode == "overwrite":
            writer = writer.mode("overwrite").option("overwriteSchema", "true")
        else:
            writer = writer.mode("append") if normalized_mode == "append" else writer.mode(mode_value)
        LOGGER.info(
            "Usando saveAsTable para Lakehouse",
            extra={
                "platform": "fabric",
                "event": "fabric_table_saveas",
                "schema": schema,
                "table": table,
                "mode": normalized_mode,
                "uri": table_uri,
            },
        )
        writer.saveAsTable(f"{schema}.{table}")
    else:
        if is_tables_uri:
            LOGGER.warning(
                "No se pudo interpretar la ruta de tabla de Fabric",
                extra={
                    "platform": "fabric",
                    "event": "fabric_table_parse_failed",
                    "uri": table_uri,
                },
            )
        LOGGER.info(
            "Escritura estilo archivos para Fabric",
            extra={
                "platform": "fabric",
                "event": "fabric_file_sink",
                "uri": table_uri,
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
