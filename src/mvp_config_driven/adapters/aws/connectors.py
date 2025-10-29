"""Helpers for building JDBC connectors backed by AWS metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Optional, Sequence

import boto3

from .secrets_manager import SecretsManagerAdapter


@dataclass(frozen=True)
class JdbcConnection:
    """Resolved JDBC connection settings."""

    url: str
    driver: str
    properties: Mapping[str, Any]


class RedshiftConnector:
    """Utility helper to build JDBC metadata for Amazon Redshift."""

    def __init__(
        self,
        *,
        cluster_id: str,
        database: str,
        secret_id: str,
        schema: str = "public",
        session: boto3.session.Session | None = None,
        secrets_adapter: SecretsManagerAdapter | None = None,
    ) -> None:
        self._cluster_id = cluster_id
        self._database = database
        self._schema = schema
        self._session = session or boto3.session.Session()
        self._secrets = secrets_adapter or SecretsManagerAdapter(session=self._session)
        self._secret_id = secret_id

    @property
    def schema(self) -> str:
        return self._schema

    def jdbc_connection(self) -> JdbcConnection:
        credentials = self._load_credentials()
        cluster = self._describe_cluster()
        endpoint = cluster["Endpoint"]
        url = f"jdbc:redshift://{endpoint['Address']}:{endpoint['Port']}/{self._database}"
        properties = {
            "user": credentials.get("username") or credentials.get("user"),
            "password": credentials.get("password"),
        }
        return JdbcConnection(
            url=url,
            driver="com.amazon.redshift.jdbc.Driver",
            properties=properties,
        )

    def copy_statement(
        self,
        *,
        table: str,
        s3_path: str,
        iam_role: Optional[str] = None,
        format_hint: Optional[str] = None,
        additional_options: Mapping[str, Any] | None = None,
    ) -> str:
        """Return a Redshift ``COPY`` command for incremental appends."""

        options: list[str] = []
        if format_hint:
            options.append(str(format_hint))
        for key, value in (additional_options or {}).items():
            rendered = f"{str(key).upper()} {value}"
            options.append(rendered)

        lines = [f"COPY {table} FROM '{s3_path}'"]
        if iam_role:
            lines.append(f"    IAM_ROLE '{iam_role}'")
        for entry in options:
            lines.append(f"    {entry}")
        return "\n".join(lines)

    def merge_statement(
        self,
        *,
        table: str,
        staging_table: str,
        key_columns: Sequence[str],
        update_columns: Sequence[str],
    ) -> str:
        """Generate a ``MERGE`` statement suitable for incremental merges."""

        if not key_columns:
            raise ValueError("merge_statement requires at least one key column")
        if not update_columns:
            raise ValueError("merge_statement requires at least one update column")

        key_condition = " AND ".join(f"t.{col} = s.{col}" for col in key_columns)
        update_clause = ", ".join(f"{col} = s.{col}" for col in update_columns)
        all_columns = list(key_columns) + list(update_columns)
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join(f"s.{col}" for col in all_columns)

        return (
            f"MERGE INTO {table} AS t\n"
            f"USING {staging_table} AS s\n"
            f"ON {key_condition}\n"
            f"WHEN MATCHED THEN UPDATE SET {update_clause}\n"
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values});"
        )

    def _describe_cluster(self) -> MutableMapping[str, Any]:
        client = self._session.client("redshift")
        response = client.describe_clusters(ClusterIdentifier=self._cluster_id)
        clusters = response.get("Clusters") or []
        if not clusters:
            raise RuntimeError(f"Redshift cluster '{self._cluster_id}' not found")
        return clusters[0]

    def _load_credentials(self) -> Mapping[str, Any]:
        secret = self._secrets.get_secret(self._secret_id)
        if not secret:
            raise RuntimeError(f"Secret '{self._secret_id}' not found")
        data = json.loads(secret)
        if not isinstance(data, Mapping):
            raise RuntimeError("Secret must contain a JSON object")
        return data


__all__ = ["JdbcConnection", "RedshiftConnector"]
