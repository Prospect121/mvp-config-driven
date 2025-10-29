"""Azure-backed orchestration adapters relying on managed services."""

from __future__ import annotations

from typing import Any, Dict, Mapping

from ..local import LocalStreamingAdapter, LocalTablesAdapter, LocalStorageAdapter
from ...ports import SecretsPort, StoragePort
from .job_databricks import DatabricksJobBackend
from .secret_keyvault import KeyVaultSecretsAdapter
from .storage_adls import AdlsStorageAdapter


def _build_storage_adapter(config: Mapping[str, Any]) -> StoragePort:
    if isinstance(config, Mapping):
        account_name = config.get("account_name")
        credential = config.get("credential") or config.get("account_key")
        sas_token = config.get("sas_token")
        auth = config.get("auth") or config.get("authentication")
        endpoint_suffix = config.get("endpoint_suffix") or "dfs.core.windows.net"
        managed_identity_cfg = config.get("managed_identity") if isinstance(config.get("managed_identity"), Mapping) else {}
        managed_identity_client_id = (
            managed_identity_cfg.get("client_id")
            if isinstance(managed_identity_cfg, Mapping)
            else None
        )
        try:
            return AdlsStorageAdapter(
                account_name=account_name,
                credential=credential,
                sas_token=sas_token,
                auth=str(auth) if auth else None,
                managed_identity_client_id=managed_identity_client_id,
                endpoint_suffix=str(endpoint_suffix),
            )
        except ImportError:
            return LocalStorageAdapter()
    return LocalStorageAdapter()


def _build_secrets_adapter(config: Mapping[str, Any]) -> SecretsPort:
    if isinstance(config, Mapping):
        vault_url = config.get("vault_url") or config.get("url")
        credential = config.get("credential")
        auth = config.get("auth") or config.get("authentication")
        managed_identity_cfg = config.get("managed_identity") if isinstance(config.get("managed_identity"), Mapping) else {}
        managed_identity_client_id = (
            managed_identity_cfg.get("client_id")
            if isinstance(managed_identity_cfg, Mapping)
            else None
        )
        try:
            return KeyVaultSecretsAdapter(
                vault_url=str(vault_url) if vault_url else None,
                credential=credential,
                auth=str(auth) if auth else None,
                managed_identity_client_id=managed_identity_client_id,
            )
        except ImportError:
            return KeyVaultSecretsAdapter(vault_url=None)
    return KeyVaultSecretsAdapter(vault_url=None)


def _build_job_backend(config: Mapping[str, Any]) -> DatabricksJobBackend:
    if not isinstance(config, Mapping):
        return DatabricksJobBackend()
    cli_path = config.get("cli_path")
    profile = config.get("profile")
    host = config.get("host")
    token = config.get("token")
    default_job_id = config.get("job_id") or config.get("default_job_id")
    default_pipeline_id = (
        config.get("pipeline_id") or config.get("default_pipeline_id")
    )
    workspace_cfg = config.get("workspace") if isinstance(config.get("workspace"), Mapping) else {}
    if isinstance(workspace_cfg, Mapping):
        host = host or workspace_cfg.get("host")
        profile = profile or workspace_cfg.get("profile")
        token = token or workspace_cfg.get("token")
    return DatabricksJobBackend(
        cli_path=str(cli_path) if cli_path else "databricks",
        profile=str(profile) if profile else None,
        host=str(host) if host else None,
        token=str(token) if token else None,
        default_job_id=str(default_job_id) if default_job_id else None,
        default_pipeline_id=str(default_pipeline_id) if default_pipeline_id else None,
    )


def build_default_adapters(config: Mapping[str, object] | None = None) -> Dict[str, object]:
    """Instantiate Azure-backed adapters honoring the provided ``config``."""

    config = config or {}
    storage_cfg = config.get("storage") if isinstance(config.get("storage"), Mapping) else {}
    secrets_cfg = config.get("secrets") if isinstance(config.get("secrets"), Mapping) else {}
    job_cfg = config.get("job_backend") if isinstance(config.get("job_backend"), Mapping) else {}

    storage = _build_storage_adapter(storage_cfg)
    secrets = _build_secrets_adapter(secrets_cfg)
    job_backend = _build_job_backend(job_cfg)

    tables = LocalTablesAdapter(storage=storage)

    return {
        "storage": storage,
        "tables": tables,
        "secrets": secrets,
        "job_backend": job_backend,
        "streaming": LocalStreamingAdapter(),
    }


__all__ = ["build_default_adapters"]
