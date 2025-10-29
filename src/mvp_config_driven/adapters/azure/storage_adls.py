"""Azure Data Lake Storage adapter built on top of ``azure-storage-file-datalake``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

try:  # pragma: no cover - optional dependency in CI
    from azure.core.exceptions import ResourceNotFoundError  # type: ignore
except Exception:  # pragma: no cover - azure SDK optional
    class ResourceNotFoundError(Exception):  # type: ignore[override]
        """Fallback error used when Azure SDK is unavailable."""

        pass

try:  # pragma: no cover - optional dependency in CI
    from azure.identity import ManagedIdentityCredential  # type: ignore
except Exception:  # pragma: no cover - azure identity optional
    ManagedIdentityCredential = None  # type: ignore

try:  # pragma: no cover - optional dependency in CI
    from azure.storage.filedatalake import DataLakeServiceClient  # type: ignore
except Exception:  # pragma: no cover - azure storage optional
    DataLakeServiceClient = None  # type: ignore

from ...ports import StoragePort


@dataclass
class _FileClientWrapper:
    """Lightweight wrapper used by tests to emulate file client interactions."""

    file_client: object

    def read_text(self) -> str:
        downloader = getattr(self.file_client, "download_file")()
        payload = downloader.readall()
        if isinstance(payload, bytes):
            return payload.decode("utf-8")
        return str(payload)

    def write_text(self, content: str) -> None:
        self.file_client.upload_data(content.encode("utf-8"), overwrite=True)

    def exists(self) -> bool:
        try:
            self.file_client.get_file_properties()
        except ResourceNotFoundError:
            return False
        return True


class AdlsStorageAdapter(StoragePort):
    """Storage adapter that reads configuration files from ADLS Gen2."""

    def __init__(
        self,
        *,
        account_name: str | None = None,
        credential: object | None = None,
        sas_token: str | None = None,
        auth: str | None = None,
        managed_identity_client_id: str | None = None,
        endpoint_suffix: str = "dfs.core.windows.net",
        service_client: object | None = None,
    ) -> None:
        self._service_client = service_client or self._build_service_client(
            account_name=account_name,
            credential=credential,
            sas_token=sas_token,
            auth=auth,
            managed_identity_client_id=managed_identity_client_id,
            endpoint_suffix=endpoint_suffix,
        )

    def _build_service_client(
        self,
        *,
        account_name: str | None,
        credential: object | None,
        sas_token: str | None,
        auth: str | None,
        managed_identity_client_id: str | None,
        endpoint_suffix: str,
    ) -> object:
        if self._is_provided(self, "_service_client"):
            return getattr(self, "_service_client")
        if account_name is None:
            raise ValueError("account_name is required when service_client is not provided")
        if DataLakeServiceClient is None:
            raise ImportError(
                "azure-storage-file-datalake is required to build an ADLS adapter"
            )
        credential_obj = self._resolve_credential(
            credential=credential,
            sas_token=sas_token,
            auth=auth,
            managed_identity_client_id=managed_identity_client_id,
        )
        account_url = f"https://{account_name}.{endpoint_suffix}".rstrip("/")
        if credential_obj is None:
            return DataLakeServiceClient(account_url=account_url)
        return DataLakeServiceClient(account_url=account_url, credential=credential_obj)

    @staticmethod
    def _resolve_credential(
        *,
        credential: object | None,
        sas_token: str | None,
        auth: str | None,
        managed_identity_client_id: str | None,
    ) -> object | None:
        if auth:
            normalized = auth.strip().lower()
            if normalized in {"msi", "managed_identity"}:
                if ManagedIdentityCredential is None:
                    raise ImportError("azure-identity is required for managed identity auth")
                return ManagedIdentityCredential(client_id=managed_identity_client_id)
            if normalized == "sas" and sas_token:
                return sas_token
        if sas_token and not credential:
            return sas_token
        return credential

    @staticmethod
    def _is_provided(obj: object, attr: str) -> bool:
        return hasattr(obj, attr) and getattr(obj, attr) is not None

    def _resolve_file_client(self, uri: str) -> _FileClientWrapper:
        filesystem, path = self._split_uri(uri)
        client = self._service_client.get_file_system_client(filesystem)
        file_client = client.get_file_client(path)
        return _FileClientWrapper(file_client)

    def read_text(self, uri: str) -> str:
        wrapper = self._resolve_file_client(uri)
        return wrapper.read_text()

    def exists(self, uri: str) -> bool:
        wrapper = self._resolve_file_client(uri)
        return wrapper.exists()

    def write_text(self, uri: str, content: str) -> None:
        wrapper = self._resolve_file_client(uri)
        wrapper.write_text(content)

    @staticmethod
    def _split_uri(uri: str) -> Tuple[str, str]:
        if not uri.startswith("abfss://"):
            raise ValueError(f"Unsupported URI: {uri}")
        path = uri[8:]
        filesystem, _, remainder = path.partition("@")
        if not remainder:
            raise ValueError(f"Invalid ABFSS URI: {uri}")
        _, _, relative_path = remainder.partition("/")
        return filesystem, relative_path


__all__ = ["AdlsStorageAdapter"]
