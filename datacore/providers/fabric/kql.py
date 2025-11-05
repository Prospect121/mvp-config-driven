"""Integraciones KQL/Eventhouse para Microsoft Fabric."""

from __future__ import annotations

import logging
from typing import Any, Dict

from .env import FabricEnv

LOGGER = logging.getLogger(__name__)


def kql_read(env: FabricEnv, statement: str, options: Dict[str, Any] | None = None):
    """Ejecuta una consulta KQL contra Eventhouse/Kusto."""

    options = options or {}
    cluster = options.get("cluster") or getattr(env, "eventhouse_id", None)
    database = options.get("database")
    if not cluster or not database:
        raise ValueError("cluster y database son obligatorios para kql_read")
    try:
        from azure.kusto.data import KustoClient, KustoConnectionStringBuilder  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependencias opcionales
        raise RuntimeError(
            "Faltan dependencias azure-kusto-data. Instala con: pip install .[fabric]"
        ) from exc
    builder = KustoConnectionStringBuilder.with_aad_token_provider(
        cluster,
        lambda: env.get_token(),
    )
    client = KustoClient(builder)
    LOGGER.info(
        "Ejecutando kql_read",
        extra={"cluster": cluster, "database": database, "workspace_id": env.workspace_id},
    )
    return client.execute(database, statement)


def kql_ingest(env: FabricEnv, table: str, df, options: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Ingesta un DataFrame en Eventhouse mediante cola."""

    options = options or {}
    cluster = options.get("cluster") or getattr(env, "eventhouse_id", None)
    database = options.get("database")
    if not cluster or not database:
        raise ValueError("cluster y database son obligatorios para kql_ingest")
    try:
        from azure.kusto.ingest import (  # type: ignore
            IngestionProperties,
            QueuedIngestClient,
        )
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Faltan dependencias azure-kusto-ingest. Instala con: pip install .[fabric]"
        ) from exc
    try:
        from azure.kusto.data import KustoConnectionStringBuilder  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Faltan dependencias azure-kusto-data. Instala con: pip install .[fabric]"
        ) from exc
    builder = KustoConnectionStringBuilder.with_aad_token_provider(
        cluster,
        lambda: env.get_token(),
    )
    ingest_client = QueuedIngestClient(builder)
    properties = IngestionProperties(database=database, table=table)
    LOGGER.info(
        "Ejecutando kql_ingest",
        extra={"cluster": cluster, "database": database, "table": table},
    )
    ingest_client.ingest_from_dataframe(df, ingestion_properties=properties)
    return {"table": table, "database": database}
