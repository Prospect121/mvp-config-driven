"""Conector para Apache Iceberg usando Spark SQL."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)


def _full_table_name(catalog: str, namespace: str, table: str) -> str:
    """Construye nombre completo de tabla: catalog.namespace.table"""
    return f"{catalog}.{namespace}.{table}"


def table_exists(spark: SparkSession, catalog: str, namespace: str, table: str) -> bool:
    """Verifica si una tabla Iceberg existe."""
    full_name = _full_table_name(catalog, namespace, table)
    try:
        spark.sql(f"DESCRIBE TABLE {full_name}")
        return True
    except Exception:
        return False


def create_namespace_if_not_exists(spark: SparkSession, catalog: str, namespace: str) -> None:
    """Crea el namespace si no existe."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


# ========== LECTURA ==========


def read(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    table: str,
    snapshot_id: int | None = None,
    as_of_timestamp: str | None = None,
    options: dict[str, Any] | None = None,
) -> DataFrame:
    """Lee una tabla Iceberg con soporte para time travel.

    Args:
        spark: SparkSession activa
        catalog: Nombre del catálogo Iceberg
        namespace: Namespace/database dentro del catálogo
        table: Nombre de la tabla
        snapshot_id: ID de snapshot para time travel (opcional)
        as_of_timestamp: Timestamp para time travel, formato 'YYYY-MM-DD HH:MM:SS' (opcional)
        options: Opciones adicionales de lectura (opcional)

    Returns:
        DataFrame con los datos de la tabla
    """
    full_name = _full_table_name(catalog, namespace, table)

    # Usar formato iceberg para aplicar options correctamente
    reader = spark.read.format("iceberg")

    # Aplicar options si existen
    if options:
        for k, v in options.items():
            reader = reader.option(k, str(v))

    # Aplicar time travel si se especifica
    if snapshot_id:
        LOGGER.info("Leyendo tabla Iceberg con snapshot_id=%s: %s", snapshot_id, full_name)
        reader = reader.option("snapshot-id", snapshot_id)
    elif as_of_timestamp:
        LOGGER.info("Leyendo tabla Iceberg as_of_timestamp=%s: %s", as_of_timestamp, full_name)
        reader = reader.option("as-of-timestamp", as_of_timestamp)
    else:
        LOGGER.info("Leyendo tabla Iceberg: %s", full_name)

    return reader.load(full_name)


# ========== ESCRITURA ==========


def write(
    df: DataFrame,
    catalog: str,
    namespace: str,
    table: str,
    mode: str = "append",
    partition_by: list[str] | None = None,
    properties: dict[str, str] | None = None,
) -> None:
    """Escribe a una tabla Iceberg con CREATE TABLE IF NOT EXISTS.

    Args:
        df: DataFrame a escribir
        catalog: Nombre del catálogo Iceberg
        namespace: Namespace/database dentro del catálogo
        table: Nombre de la tabla
        mode: Modo de escritura: 'append', 'overwrite', 'replace'
        partition_by: Lista de columnas para particionar
        properties: Propiedades de la tabla Iceberg
    """
    spark = df.sparkSession
    full_name = _full_table_name(catalog, namespace, table)

    if not table_exists(spark, catalog, namespace, table):
        create_namespace_if_not_exists(spark, catalog, namespace)
        writer = df.writeTo(full_name).using("iceberg")
        if partition_by:
            writer = writer.partitionedBy(*partition_by)
        if properties:
            for k, v in properties.items():
                writer = writer.tableProperty(k, str(v))
        writer.create()
        LOGGER.info("Tabla Iceberg creada: %s", full_name)
        return

    writer = df.writeTo(full_name)
    if mode == "overwrite":
        # Reemplazo total de la tabla
        writer.replace()
    elif mode in ("overwrite_partitions", "replace_partitions"):
        # Solo sobrescribir particiones afectadas por los datos
        writer.overwritePartitions()
    elif mode == "replace":
        writer.replace()
    else:
        writer.append()
    LOGGER.info("Datos escritos a tabla Iceberg: %s (mode=%s)", full_name, mode)


# ========== MERGE (UPSERT) ==========


def merge(
    df: DataFrame,
    catalog: str,
    namespace: str,
    table: str,
    keys: list[str],
    properties: dict[str, str] | None = None,
    partition_by: list[str] | None = None,
) -> None:
    """Ejecuta MERGE INTO para upsert.

    Args:
        df: DataFrame con los datos a mergear
        catalog: Nombre del catálogo Iceberg
        namespace: Namespace/database dentro del catálogo
        table: Nombre de la tabla
        keys: Lista de columnas clave para el merge
        properties: Propiedades de la tabla (solo si se crea nueva)
        partition_by: Lista de columnas para particionar (solo si se crea nueva)
    """
    spark = df.sparkSession
    full_name = _full_table_name(catalog, namespace, table)

    if not table_exists(spark, catalog, namespace, table):
        create_namespace_if_not_exists(spark, catalog, namespace)
        writer = df.writeTo(full_name).using("iceberg")
        if partition_by:
            writer = writer.partitionedBy(*partition_by)
        if properties:
            for k, v in properties.items():
                writer = writer.tableProperty(k, str(v))
        writer.create()
        LOGGER.info("Tabla Iceberg creada (merge inicial): %s", full_name)
        return

    source_view = "__iceberg_merge_source"
    df.createOrReplaceTempView(source_view)

    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    columns = df.columns
    non_key_columns = [c for c in columns if c not in keys]

    if non_key_columns:
        update_set = ", ".join([f"t.{c} = s.{c}" for c in non_key_columns])
        update_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
    else:
        update_clause = ""

    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"s.{c}" for c in columns])

    merge_sql = f"""
    MERGE INTO {full_name} AS t
    USING {source_view} AS s
    ON {merge_condition}
    {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    spark.sql(merge_sql)
    LOGGER.info("MERGE ejecutado en tabla Iceberg: %s", full_name)


# ========== MÉTRICAS ==========


def write_metrics(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    metrics: dict[str, Any],
) -> None:
    """Escribe métricas a una tabla Iceberg centralizada.

    Args:
        spark: SparkSession activa
        catalog: Nombre del catálogo Iceberg
        namespace: Namespace donde crear la tabla de métricas
        metrics: Diccionario con métricas de la ejecución
    """
    metrics_table = "_pipeline_metrics"

    metrics_json = json.dumps(metrics, default=str)
    metrics_df = spark.createDataFrame(
        [
            {
                "run_id": str(metrics.get("run_id", "")),
                "dataset": str(metrics.get("dataset", "")),
                "layer": str(metrics.get("layer", "")),
                "environment": str(metrics.get("environment", "")),
                "timestamp_utc": str(metrics.get("timestamp_utc", "")),
                "input_rows": int(metrics.get("input_rows", 0)),
                "valid_rows": int(metrics.get("valid_rows", 0)),
                "invalid_rows": int(metrics.get("invalid_rows", 0)),
                "quarantine_rows": int(metrics.get("quarantine_rows", 0)),
                "metrics_json": metrics_json,
            }
        ]
    )

    write(metrics_df, catalog, namespace, metrics_table, mode="append")
    LOGGER.info("Métricas escritas a %s.%s.%s", catalog, namespace, metrics_table)


# ========== REJECTS ==========


def write_rejects(
    df: DataFrame,
    catalog: str,
    namespace: str,
    source_table: str,
    run_id: str,
) -> None:
    """Escribe registros rechazados a una tabla Iceberg.

    Args:
        df: DataFrame con registros rechazados
        catalog: Nombre del catálogo Iceberg
        namespace: Namespace donde crear la tabla de rejects
        source_table: Nombre de la tabla origen (para nombrar la tabla de rejects)
        run_id: ID de la ejecución actual
    """
    rejects_table = f"_rejects_{source_table}"

    df_with_meta = df.withColumn("_reject_run_id", F.lit(run_id)).withColumn(
        "_reject_timestamp", F.current_timestamp()
    )

    write(df_with_meta, catalog, namespace, rejects_table, mode="append")
    LOGGER.info(
        "Rejects escritos a %s.%s.%s (%d registros)",
        catalog,
        namespace,
        rejects_table,
        df.count(),
    )


# ========== MANTENIMIENTO ==========


def expire_snapshots(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    table: str,
    older_than: str = "7 days",
) -> None:
    """Elimina snapshots antiguos para liberar espacio.

    Args:
        spark: SparkSession activa
        catalog: Nombre del catálogo Iceberg
        namespace: Namespace de la tabla
        table: Nombre de la tabla
        older_than: Antigüedad mínima para eliminar (ej: '7 days', '1 hour')
    """
    full_name = _full_table_name(catalog, namespace, table)
    spark.sql(
        f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{full_name}',
            older_than => TIMESTAMP '{older_than}'
        )
    """
    )
    LOGGER.info("Snapshots expirados en: %s (older_than=%s)", full_name, older_than)


def rewrite_data_files(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    table: str,
) -> None:
    """Compacta archivos pequeños en archivos más grandes.

    Args:
        spark: SparkSession activa
        catalog: Nombre del catálogo Iceberg
        namespace: Namespace de la tabla
        table: Nombre de la tabla
    """
    full_name = _full_table_name(catalog, namespace, table)
    spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{full_name}')")
    LOGGER.info("Data files reescritos en: %s", full_name)
