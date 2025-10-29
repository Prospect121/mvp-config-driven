from __future__ import annotations

import json

import boto3
import yaml
from moto import mock_aws

from mvp_config_driven.adapters.aws import AwsTablesAdapter
from mvp_config_driven.adapters.aws.connectors import RedshiftConnector
from mvp_config_driven.adapters.aws.job_glue import parse_awscli_arguments
from mvp_config_driven.adapters.aws.storage_s3 import S3StorageAdapter


@mock_aws
def test_s3_storage_adapter_read_write_roundtrip() -> None:
    session = boto3.session.Session(region_name="us-east-1")
    client = session.client("s3")
    client.create_bucket(Bucket="configs")

    adapter = S3StorageAdapter(session=session)
    uri = "s3://configs/pipeline/config.yml"
    adapter.write_text(uri, "greeting: hello")

    assert adapter.exists(uri)
    assert adapter.read_text(uri) == "greeting: hello"


@mock_aws
def test_parse_awscli_arguments() -> None:
    parsed = parse_awscli_arguments(["--layer", "raw", "--env.PRODI_FORCE_DRY_RUN", "true"])
    assert parsed == {"layer": "raw", "env.PRODI_FORCE_DRY_RUN": "true"}


def _bootstrap_redshift(session: boto3.session.Session, *, cluster_id: str = "demo") -> None:
    client = session.client("redshift")
    client.create_cluster(
        ClusterIdentifier=cluster_id,
        ClusterType="multi-node",
        MasterUsername="admin",
        MasterUserPassword="SuperSecret1",
        NodeType="dc2.large",
        DBName="dev",
        NumberOfNodes=1,
    )


@mock_aws
def test_redshift_connector_builds_incremental_statements() -> None:
    session = boto3.session.Session(region_name="us-east-1")
    _bootstrap_redshift(session)

    secrets = session.client("secretsmanager")
    secrets.create_secret(
        Name="redshift/demo",
        SecretString=json.dumps({"username": "dbuser", "password": "dbpass"}),
    )

    connector = RedshiftConnector(
        session=session,
        cluster_id="demo",
        database="analytics",
        secret_id="redshift/demo",
        schema="analytics",
    )

    jdbc = connector.jdbc_connection()
    assert jdbc.driver == "com.amazon.redshift.jdbc.Driver"
    assert jdbc.properties["user"] == "dbuser"

    copy_sql = connector.copy_statement(
        table="analytics.events",
        s3_path="s3://configs/staging/events/",
        iam_role="arn:aws:iam::123456789012:role/Copy",
        format_hint="JSON 'auto'",
        additional_options={"truncatecolumns": "true"},
    )
    assert "COPY analytics.events" in copy_sql
    assert "IAM_ROLE 'arn:aws:iam::123456789012:role/Copy'" in copy_sql
    assert "JSON 'auto'" in copy_sql
    assert "TRUNCATECOLUMNS true" in copy_sql

    merge_sql = connector.merge_statement(
        table="analytics.events",
        staging_table="staging_events",
        key_columns=["event_id"],
        update_columns=["amount", "updated_at"],
    )
    assert "MERGE INTO analytics.events" in merge_sql
    assert "USING staging_events" in merge_sql
    assert "event_id = s.event_id" in merge_sql
    assert "amount = s.amount" in merge_sql
    assert "INSERT (event_id, amount, updated_at)" in merge_sql


@mock_aws
def test_aws_tables_adapter_enriches_table_settings() -> None:
    session = boto3.session.Session(region_name="us-east-1")
    client = session.client("s3")
    client.create_bucket(Bucket="configs")

    _bootstrap_redshift(session, cluster_id="cluster-1")

    secrets = session.client("secretsmanager")
    secrets.create_secret(
        Name="redshift/cluster-1",
        SecretString=json.dumps({"username": "dbuser", "password": "dbpass"}),
    )

    metadata = {
        "table_settings": {"owner": "analytics"},
        "connectors": {
            "redshift": {
                "cluster_id": "cluster-1",
                "database": "analytics",
                "secret_id": "redshift/cluster-1",
                "schema": "analytics",
                "table": "analytics.events",
                "staging_path": "s3://configs/staging/events/",
                "iam_role": "arn:aws:iam::123456789012:role/Copy",
                "merge": {
                    "staging_table": "staging_events",
                    "key_columns": ["event_id"],
                    "update_columns": ["amount", "updated_at"],
                    "copy_options": {"truncatecolumns": "true"},
                    "format": "JSON 'auto'",
                },
            }
        },
    }

    client.put_object(
        Bucket="configs",
        Key="metadata.yml",
        Body=yaml.safe_dump(metadata).encode("utf-8"),
    )

    adapter = AwsTablesAdapter(session=session)
    manager, table_settings = adapter.load_metadata("s3://configs/metadata.yml", environment="prod")

    assert manager is None
    assert table_settings["owner"] == "analytics"

    redshift_settings = table_settings["connectors"]["redshift"]
    assert redshift_settings["jdbc"]["properties"]["user"] == "dbuser"
    assert "COPY analytics.events" in redshift_settings["append_sql"]
    assert "MERGE INTO analytics.events" in redshift_settings["merge_sql"]
