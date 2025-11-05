"""Gestión de secretos multi-cloud."""

from __future__ import annotations

import base64
import os
from typing import Optional
from urllib.parse import urlparse


class SecretNotFoundError(RuntimeError):
    """Se lanza cuando un secreto no existe."""


def get_secret(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise SecretNotFoundError(f"No se encontró el secreto {name}")
    return value


def resolve_secret(uri: str) -> str:
    """Resuelve URIs secret:// utilizando el backend apropiado."""

    parsed = urlparse(uri)
    if parsed.scheme != "secret":
        raise ValueError(f"URI de secreto inválida: {uri}")
    backend = parsed.netloc
    path = parsed.path.lstrip("/")
    if backend == "kv":
        return _resolve_azure_kv(path)
    if backend == "sm":
        return _resolve_aws_sm(path)
    if backend == "gcp":
        return _resolve_gcp_secret(path)
    raise RuntimeError(f"Backend de secretos desconocido: {backend}")


def _resolve_azure_kv(path: str) -> str:
    parts = [p for p in path.split("/") if p]
    if len(parts) < 2:
        raise ValueError("Se requiere vault/secret en secret://kv/<vault>/<secret>")
    vault_name, secret_name, *rest = parts
    version: Optional[str] = rest[0] if rest else None
    try:
        from azure.identity import DefaultAzureCredential  # type: ignore
        from azure.keyvault.secrets import SecretClient  # type: ignore
    except ImportError as exc:  # pragma: no cover - depende de extras
        raise RuntimeError(
            "Faltan dependencias de Azure Key Vault. Instala con: pip install .[fabric]"
        ) from exc
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=f"https://{vault_name}.vault.azure.net", credential=credential)
    secret = client.get_secret(secret_name, version=version)
    return secret.value


def _resolve_aws_sm(path: str) -> str:
    secret_id = path
    try:
        import boto3  # type: ignore
    except ImportError as exc:  # pragma: no cover - depende del entorno
        raise RuntimeError(
            "Falta boto3 para resolver secret://sm/. Instala boto3 o configure el backend"
        ) from exc
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_id)
    if "SecretString" in response:
        return response["SecretString"]
    if "SecretBinary" in response:
        return base64.b64decode(response["SecretBinary"]).decode("utf-8")
    raise SecretNotFoundError(f"Secret {secret_id} vacío en AWS Secrets Manager")


def _resolve_gcp_secret(path: str) -> str:
    parts = [p for p in path.split("/") if p]
    if len(parts) < 2:
        raise ValueError("Se requiere project/secret en secret://gcp/<project>/<secret>")
    project_id, secret_id, *rest = parts
    version = rest[0] if rest else "latest"
    try:
        from google.cloud import secretmanager  # type: ignore
    except ImportError as exc:  # pragma: no cover - depende de extras
        raise RuntimeError(
            "Falta google-cloud-secret-manager para secret://gcp/. Instálalo antes de usarlo"
        ) from exc
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("utf-8")
