"""Azure Key Vault backed secrets adapter."""

from __future__ import annotations

from typing import Optional

try:  # pragma: no cover - optional dependency in CI
    from azure.core.exceptions import ResourceNotFoundError  # type: ignore
except Exception:  # pragma: no cover - azure sdk optional
    class ResourceNotFoundError(Exception):  # type: ignore[override]
        pass

try:  # pragma: no cover - optional dependency in CI
    from azure.identity import ManagedIdentityCredential  # type: ignore
except Exception:  # pragma: no cover - azure identity optional
    ManagedIdentityCredential = None  # type: ignore

try:  # pragma: no cover - optional dependency in CI
    from azure.keyvault.secrets import SecretClient  # type: ignore
except Exception:  # pragma: no cover - azure keyvault optional
    SecretClient = None  # type: ignore

from ...ports import SecretsPort


class _NullSecretClient:
    """Fallback client used when no Key Vault configuration is provided."""

    def get_secret(self, key: str) -> object:  # pragma: no cover - trivial fallback
        raise ResourceNotFoundError()


class KeyVaultSecretsAdapter(SecretsPort):
    """Secrets adapter that retrieves secrets from Azure Key Vault."""

    def __init__(
        self,
        *,
        vault_url: str | None,
        credential: object | None = None,
        auth: str | None = None,
        managed_identity_client_id: str | None = None,
        secret_client: object | None = None,
    ) -> None:
        self._vault_url = vault_url
        if secret_client is not None:
            self._secret_client = secret_client
        elif not vault_url:
            self._secret_client = _NullSecretClient()
        else:
            self._secret_client = self._build_client(
                vault_url=vault_url,
                credential=credential,
                auth=auth,
                managed_identity_client_id=managed_identity_client_id,
            )

    def _build_client(
        self,
        *,
        vault_url: str | None,
        credential: object | None,
        auth: str | None,
        managed_identity_client_id: str | None,
    ) -> object:
        if not vault_url:
            raise ValueError("vault_url is required when secret_client is not provided")
        if SecretClient is None:
            raise ImportError("azure-keyvault-secrets is required for Key Vault support")
        credential_obj = self._resolve_credential(
            credential=credential,
            auth=auth,
            managed_identity_client_id=managed_identity_client_id,
        )
        if credential_obj is None:
            return SecretClient(vault_url=vault_url)
        return SecretClient(vault_url=vault_url, credential=credential_obj)

    @staticmethod
    def _resolve_credential(
        *,
        credential: object | None,
        auth: str | None,
        managed_identity_client_id: str | None,
    ) -> object | None:
        if auth and auth.strip().lower() in {"msi", "managed_identity"}:
            if ManagedIdentityCredential is None:
                raise ImportError("azure-identity is required for managed identity auth")
            return ManagedIdentityCredential(client_id=managed_identity_client_id)
        return credential

    def get_secret(self, key: str) -> Optional[str]:
        if not key:
            return None
        try:
            secret = self._secret_client.get_secret(key)
        except ResourceNotFoundError:
            return None
        return getattr(secret, "value", None)


__all__ = ["KeyVaultSecretsAdapter"]
