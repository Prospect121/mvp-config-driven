"""AWS-backed orchestration adapters leveraging boto3 primitives."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import boto3
import yaml

from ...ports import SecretsPort, StoragePort, TablesPort
from ..local import LocalStreamingAdapter
from .connectors import RedshiftConnector
from .job_glue import GlueJobBackend, parse_awscli_arguments
from .secrets_manager import SecretsManagerAdapter
from .storage_s3 import S3StorageAdapter


class AwsTablesAdapter(TablesPort):
    """Tables adapter that enriches metadata using AWS services when available."""

    def __init__(
        self,
        *,
        session: boto3.session.Session | None = None,
        storage: StoragePort | None = None,
        secrets: SecretsPort | None = None,
    ) -> None:
        self._session = session or boto3.session.Session()
        self._storage = storage or S3StorageAdapter(session=self._session)
        self._secrets = secrets or SecretsManagerAdapter(session=self._session)

    def load_metadata(self, path: str, environment: str) -> tuple[Any, Dict[str, Any]]:
        payload = yaml.safe_load(self._storage.read_text(path)) or {}
        table_settings = dict(payload.get("table_settings") or {})

        connectors_cfg = payload.get("connectors") or {}
        connectors: Dict[str, Any] = {}

        redshift_cfg = connectors_cfg.get("redshift")
        if isinstance(redshift_cfg, Mapping):
            connectors["redshift"] = self._build_redshift_settings(redshift_cfg, environment)

        if connectors:
            table_settings["connectors"] = connectors

        return None, table_settings

    def _build_redshift_settings(
        self, cfg: Mapping[str, Any], environment: str
    ) -> Dict[str, Any]:
        cluster_id = str(cfg["cluster_id"])
        database = str(cfg.get("database") or environment)
        secret_id = str(cfg["secret_id"])
        schema = str(cfg.get("schema") or "public")
        table_name = str(cfg.get("table") or f"{schema}.{cfg.get('table_name', 'dataset')}")
        staging_table = str((cfg.get("merge") or {}).get("staging_table") or "staging")

        connector = RedshiftConnector(
            cluster_id=cluster_id,
            database=database,
            secret_id=secret_id,
            schema=schema,
            session=self._session,
            secrets_adapter=self._secrets,
        )

        jdbc = connector.jdbc_connection()

        incremental_cfg_raw = cfg.get("merge") or {}
        incremental_cfg = dict(incremental_cfg_raw) if isinstance(incremental_cfg_raw, Mapping) else {}
        key_columns = [str(col) for col in incremental_cfg.get("key_columns", [])]
        update_columns = [str(col) for col in incremental_cfg.get("update_columns", [])]
        copy_options_raw = incremental_cfg.get("copy_options")
        copy_options = copy_options_raw if isinstance(copy_options_raw, Mapping) else {}
        format_hint = incremental_cfg.get("format")
        staging_path = cfg.get("staging_path")
        iam_role = cfg.get("iam_role")

        append_sql: Optional[str] = None
        if staging_path:
            append_sql = connector.copy_statement(
                table=table_name,
                s3_path=str(staging_path),
                iam_role=str(iam_role) if iam_role else None,
                format_hint=str(format_hint) if format_hint else None,
                additional_options=copy_options,
            )

        merge_sql: Optional[str] = None
        if key_columns and update_columns:
            merge_sql = connector.merge_statement(
                table=table_name,
                staging_table=staging_table,
                key_columns=key_columns,
                update_columns=update_columns,
            )

        redshift_settings: Dict[str, Any] = {
            "jdbc": {
                "url": jdbc.url,
                "driver": jdbc.driver,
                "properties": dict(jdbc.properties),
            }
        }

        if append_sql:
            redshift_settings["append_sql"] = append_sql
        if merge_sql:
            redshift_settings["merge_sql"] = merge_sql

        return redshift_settings

    def log_pipeline_execution(
        self, manager: Any, *, dataset_name: str, pipeline_type: str, status: str
    ) -> Optional[str]:  # pragma: no cover - delegated to platform observability
        return None


def _build_session(config: Mapping[str, Any] | None) -> boto3.session.Session:
    if not config:
        return boto3.session.Session()
    return boto3.session.Session(**{k: v for k, v in config.items() if v is not None})


def build_default_adapters(config: Mapping[str, object] | None = None) -> dict[str, object]:
    """Instantiate AWS-backed adapters using ``config`` hints."""

    config = config or {}
    session_cfg = config.get("session") if isinstance(config.get("session"), Mapping) else {}
    session = _build_session(session_cfg)

    storage_cfg = config.get("storage") if isinstance(config.get("storage"), Mapping) else {}
    storage = S3StorageAdapter(session=session, s3fs_options=storage_cfg.get("s3fs_options"))

    secrets = SecretsManagerAdapter(session=session)

    job_cfg = config.get("job_backend") if isinstance(config.get("job_backend"), Mapping) else {}
    cli_args = parse_awscli_arguments(job_cfg.get("cli_arguments") or [])
    default_args = (
        dict(job_cfg.get("arguments"))
        if isinstance(job_cfg.get("arguments"), Mapping)
        else {}
    )
    default_args.update(cli_args)

    job_backend = GlueJobBackend(
        session=session,
        job_name=job_cfg.get("job_name"),
        default_arguments=default_args,
    )

    tables = AwsTablesAdapter(session=session, storage=storage, secrets=secrets)

    return {
        "storage": storage,
        "tables": tables,
        "secrets": secrets,
        "job_backend": job_backend,
        "streaming": LocalStreamingAdapter(),
    }


__all__ = [
    "AwsTablesAdapter",
    "build_default_adapters",
]
