"""GCP-backed orchestration adapters leveraging Google Cloud services."""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping

from ..local import (
    LocalSecretsAdapter,
    LocalStorageAdapter,
    LocalStreamingAdapter,
    LocalTablesAdapter,
)
from ...ports import SecretsPort, StoragePort
from .job_dataproc import DataprocJobBackend
from .secret_manager import SecretManagerAdapter
from .storage_gcs import GCSStorageAdapter


def _parse_credentials(value: object) -> object | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return value


def _build_secrets_adapter(config: Mapping[str, Any]) -> SecretsPort:
    project_id = config.get("project_id") or config.get("project")
    version = config.get("version") or config.get("secret_version") or "latest"
    credentials = _parse_credentials(config.get("credentials"))
    try:
        return SecretManagerAdapter(
            project_id=str(project_id) if project_id else None,
            secret_version=str(version),
            credentials=credentials,
        )
    except ImportError:
        return LocalSecretsAdapter()


def _build_storage_adapter(config: Mapping[str, Any], secrets: SecretsPort) -> StoragePort:
    project = config.get("project")
    auth = config.get("auth") or config.get("authentication")
    credentials = _parse_credentials(config.get("credentials"))
    credentials_secret = config.get("credentials_secret") or config.get("credentialsSecret")
    options = config.get("gcsfs_options") or config.get("storage_options") or {}
    if not isinstance(options, Mapping):
        options = {}
    try:
        return GCSStorageAdapter(
            project=str(project) if project else None,
            auth=str(auth) if auth else None,
            credentials=credentials,
            credentials_secret=str(credentials_secret) if credentials_secret else None,
            secret_provider=secrets,
            gcsfs_options=options,
        )
    except ImportError:
        return LocalStorageAdapter()


def _parse_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return default


def _build_job_backend(config: Mapping[str, Any], secrets: SecretsPort) -> DataprocJobBackend:
    cli_path = config.get("cli_path") or "gcloud"
    component = config.get("component") or "dataproc"
    project = config.get("project")
    region = config.get("region")
    service_account = config.get("service_account") or config.get("serviceAccount")
    subnet = config.get("subnet") or config.get("subnetwork")
    batch_id = config.get("batch_id") or config.get("default_batch_id")
    auth = config.get("auth") or config.get("authentication")
    workload_identity_default = True if not auth else auth.strip().lower() in {
        "workload_identity",
        "workload-identity",
        "wi",
    }
    workload_identity = _parse_bool(config.get("workload_identity"), workload_identity_default)
    credentials_secret = config.get("credentials_secret") or config.get("runner_credentials_secret")
    return DataprocJobBackend(
        cli_path=str(cli_path),
        component=str(component),
        default_project=str(project) if project else None,
        default_region=str(region) if region else None,
        default_service_account=str(service_account) if service_account else None,
        default_subnet=str(subnet) if subnet else None,
        default_batch_id=str(batch_id) if batch_id else None,
        workload_identity=workload_identity,
        secret_provider=secrets,
        credentials_secret=str(credentials_secret) if credentials_secret else None,
    )


def build_default_adapters(config: Mapping[str, object] | None = None) -> Dict[str, object]:
    """Instantiate GCP-backed adapters honoring the provided ``config``."""

    config = config or {}
    secrets_cfg = config.get("secrets") if isinstance(config.get("secrets"), Mapping) else {}
    secrets = _build_secrets_adapter(secrets_cfg)

    storage_cfg = config.get("storage") if isinstance(config.get("storage"), Mapping) else {}
    storage = _build_storage_adapter(storage_cfg, secrets)

    job_cfg = config.get("job_backend") if isinstance(config.get("job_backend"), Mapping) else {}
    job_backend = _build_job_backend(job_cfg, secrets)

    tables = LocalTablesAdapter(storage=storage)

    return {
        "storage": storage,
        "tables": tables,
        "secrets": secrets,
        "job_backend": job_backend,
        "streaming": LocalStreamingAdapter(),
    }


__all__ = ["build_default_adapters"]
