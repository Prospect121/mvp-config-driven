"""Secrets adapter backed by Google Secret Manager."""

from __future__ import annotations

from typing import Dict, Optional

from ...ports import SecretsPort

try:  # pragma: no cover - optional dependency in CI environments
    from google.api_core.exceptions import NotFound  # type: ignore
except Exception:  # pragma: no cover - google api core optional
    class NotFound(Exception):  # type: ignore[override]
        """Fallback NotFound error when google API core is unavailable."""

        pass

try:  # pragma: no cover - optional dependency in CI environments
    from google.cloud import secretmanager  # type: ignore
except Exception:  # pragma: no cover - secret manager optional
    secretmanager = None  # type: ignore


class SecretManagerAdapter(SecretsPort):
    """Resolve secrets from Google Secret Manager."""

    def __init__(
        self,
        *,
        project_id: str | None = None,
        secret_version: str = "latest",
        client: object | None = None,
        credentials: object | None = None,
        cache_enabled: bool = True,
    ) -> None:
        if client is not None:
            self._client = client
        else:
            if secretmanager is None:
                raise ImportError("google-cloud-secret-manager is required for Secret Manager support")
            client_kwargs: dict[str, object] = {}
            if credentials is not None:
                client_kwargs["credentials"] = credentials
            self._client = secretmanager.SecretManagerServiceClient(**client_kwargs)
        self._project_id = project_id
        self._secret_version = secret_version or "latest"
        self._cache_enabled = cache_enabled
        self._cache: Dict[str, Optional[str]] = {}

    def get_secret(self, key: str) -> Optional[str]:
        if not key:
            return None
        if self._cache_enabled and key in self._cache:
            return self._cache[key]

        resource_name = self._resolve_resource_name(key)
        if not resource_name:
            value: Optional[str] = None
        else:
            try:
                response = self._client.access_secret_version(name=resource_name)
            except NotFound:
                value = None
            else:
                payload = getattr(response, "payload", None)
                data = getattr(payload, "data", None) if payload is not None else None
                if isinstance(data, (bytes, bytearray)):
                    value = data.decode("utf-8")
                elif data is None:
                    value = None
                else:
                    value = str(data)

        if self._cache_enabled:
            self._cache[key] = value
        return value

    def _resolve_resource_name(self, key: str) -> str | None:
        if key.startswith("projects/"):
            return key
        if not self._project_id:
            return None
        return f"projects/{self._project_id}/secrets/{key}/versions/{self._secret_version}"


__all__ = ["SecretManagerAdapter"]
