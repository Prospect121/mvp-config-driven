"""Google Cloud Storage adapter built on top of :mod:`gcsfs`."""

from __future__ import annotations

import json
from typing import Mapping

from ...ports import SecretsPort, StoragePort

try:  # pragma: no cover - optional dependency in CI environments
    import gcsfs  # type: ignore
except Exception:  # pragma: no cover - gcsfs optional
    gcsfs = None  # type: ignore

try:  # pragma: no cover - optional dependency in CI environments
    from google.cloud import storage  # type: ignore
except Exception:  # pragma: no cover - google cloud optional
    storage = None  # type: ignore

try:  # pragma: no cover - optional dependency in CI environments
    from google.oauth2 import service_account  # type: ignore
except Exception:  # pragma: no cover - service account helpers optional
    service_account = None  # type: ignore


class GCSStorageAdapter(StoragePort):
    """Storage adapter that relies on Google Cloud credentials resolution."""

    def __init__(
        self,
        *,
        project: str | None = None,
        auth: str | None = None,
        credentials: object | None = None,
        credentials_secret: str | None = None,
        secret_provider: SecretsPort | None = None,
        client: object | None = None,
        filesystem: object | None = None,
        gcsfs_options: Mapping[str, object] | None = None,
    ) -> None:
        self._auth = (auth or "").strip().lower() or None
        self._secret_provider = secret_provider
        self._credentials_secret = credentials_secret

        resolved_credentials = self._resolve_credentials(credentials)
        if resolved_credentials is None and self._should_fetch_credentials():
            secret_payload = self._fetch_credentials_payload()
            resolved_credentials = self._resolve_credentials(secret_payload)

        self._credentials = resolved_credentials

        if client is not None:
            self._client = client
        else:
            if storage is None:
                raise ImportError("google-cloud-storage is required for GCS support")
            client_kwargs: dict[str, object] = {}
            if project:
                client_kwargs["project"] = project
            if resolved_credentials is not None:
                client_kwargs["credentials"] = resolved_credentials
            self._client = storage.Client(**client_kwargs)

        options = dict(gcsfs_options or {})
        if project and "project" not in options:
            options["project"] = project
        if resolved_credentials is not None and "token" not in options:
            options["token"] = resolved_credentials

        if filesystem is not None:
            self._filesystem = filesystem
        else:
            if gcsfs is None:
                raise ImportError("gcsfs is required for GCS support")
            self._filesystem = gcsfs.GCSFileSystem(**options)

    def read_text(self, uri: str) -> str:
        with self._filesystem.open(uri, "rb") as handle:  # type: ignore[call-arg]
            data = handle.read()
        if isinstance(data, str):
            return data
        return data.decode("utf-8")

    def exists(self, uri: str) -> bool:
        bucket_name, blob_name = self._split(uri)
        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        try:
            return bool(blob.exists(self._client))
        except TypeError:  # pragma: no cover - some stubs ignore the argument
            return bool(blob.exists())

    def write_text(self, uri: str, content: str) -> None:
        bucket_name, blob_name = self._split(uri)
        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content.encode("utf-8"))

    def _should_fetch_credentials(self) -> bool:
        if not self._credentials_secret or self._secret_provider is None:
            return False
        if self._auth and self._auth not in {"service_account", "service-account"}:
            return False
        return True

    def _fetch_credentials_payload(self) -> object | None:
        if not self._credentials_secret or self._secret_provider is None:
            return None
        return self._secret_provider.get_secret(self._credentials_secret)

    @staticmethod
    def _normalize_credentials_payload(payload: object | None) -> object | None:
        if payload is None:
            return None
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return None
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return text
            return parsed
        if isinstance(payload, Mapping):
            return dict(payload)
        return payload

    def _resolve_credentials(self, payload: object | None) -> object | None:
        normalized = self._normalize_credentials_payload(payload)
        if normalized is None:
            return None
        if isinstance(normalized, Mapping) and service_account is not None:
            return service_account.Credentials.from_service_account_info(dict(normalized))
        return normalized

    @staticmethod
    def _split(uri: str) -> tuple[str, str]:
        if not uri.startswith("gs://"):
            raise ValueError(f"Unsupported URI: {uri}")
        path = uri[5:]
        bucket, _, blob = path.partition("/")
        return bucket, blob


__all__ = ["GCSStorageAdapter"]
