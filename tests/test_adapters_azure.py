import json
import sys
import types
from types import SimpleNamespace

import pytest

if "pydantic" not in sys.modules:
    fake_pydantic = types.ModuleType("pydantic")

    class _BaseModel:
        model_config = {}

        @classmethod
        def model_validate(cls, value):
            return value

        def model_dump(self, **_: object) -> dict:
            return {}

    def _field(*, default=None, default_factory=None, **__: object):
        if default_factory is not None:
            return default_factory()
        return default

    def _config_dict(**kwargs: object) -> dict:
        return dict(kwargs)

    fake_pydantic.BaseModel = _BaseModel
    fake_pydantic.Field = _field
    fake_pydantic.ConfigDict = _config_dict
    sys.modules["pydantic"] = fake_pydantic

if "fsspec" not in sys.modules:
    fake_fsspec = types.ModuleType("fsspec")

    def _filesystem(protocol: str, **_: object) -> object:  # pragma: no cover - stub
        return object()

    def _open(*args: object, **kwargs: object):  # pragma: no cover - stub
        raise FileNotFoundError()

    fake_fsspec.filesystem = _filesystem
    fake_fsspec.open = _open

    core_module = types.ModuleType("fsspec.core")

    def _url_to_fs(uri: str, **_: object):  # pragma: no cover - stub
        class _DummyFS:
            def exists(self, path: str) -> bool:
                return False

        return _DummyFS(), uri

    core_module.url_to_fs = _url_to_fs
    fake_fsspec.core = core_module
    sys.modules["fsspec"] = fake_fsspec
    sys.modules["fsspec.core"] = core_module

from mvp_config_driven.adapters.azure import secret_keyvault as secrets_module
from mvp_config_driven.adapters.azure import storage_adls as storage_module
from mvp_config_driven.adapters.azure.job_databricks import DatabricksJobBackend

KeyVaultSecretsAdapter = secrets_module.KeyVaultSecretsAdapter
KeyVaultResourceNotFoundError = secrets_module.ResourceNotFoundError
AdlsStorageAdapter = storage_module.AdlsStorageAdapter
AdlsResourceNotFoundError = storage_module.ResourceNotFoundError


class FakeDownloader:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def readall(self) -> bytes:
        return self._data


class FakeFileClient:
    def __init__(self, path: str, store: dict[str, bytes]) -> None:
        self._path = path
        self._store = store

    def download_file(self) -> FakeDownloader:
        if self._path not in self._store:
            raise AdlsResourceNotFoundError()
        return FakeDownloader(self._store[self._path])

    def upload_data(self, data: bytes, *, overwrite: bool = False) -> None:
        self._store[self._path] = data

    def get_file_properties(self) -> None:
        if self._path not in self._store:
            raise AdlsResourceNotFoundError()


class FakeFileSystemClient:
    def __init__(self, filesystem: str, store: dict[tuple[str, str], bytes]) -> None:
        self._filesystem = filesystem
        self._store = store

    def get_file_client(self, path: str) -> FakeFileClient:
        key = (self._filesystem, path)
        return FakeFileClient(path, FakeStoreProxy(self._store, key))


class FakeServiceClient:
    def __init__(self, store: dict[tuple[str, str], bytes]) -> None:
        self._store = store

    def get_file_system_client(self, filesystem: str) -> FakeFileSystemClient:
        return FakeFileSystemClient(filesystem, self._store)


class FakeStoreProxy(dict):
    def __init__(self, backing: dict[tuple[str, str], bytes], key: tuple[str, str]) -> None:
        super().__init__()
        self._backing = backing
        self._key = key

    def __getitem__(self, _: str) -> bytes:
        return self._backing[self._key]

    def __setitem__(self, _: str, value: bytes) -> None:
        self._backing[self._key] = value

    def get(self, _: str, default: bytes | None = None) -> bytes | None:
        return self._backing.get(self._key, default)

    def __contains__(self, _: object) -> bool:
        return self._key in self._backing


def test_adls_storage_adapter_roundtrip() -> None:
    store: dict[tuple[str, str], bytes] = {}
    service = FakeServiceClient(store)
    adapter = AdlsStorageAdapter(service_client=service)

    uri = "abfss://configs@contoso.dfs.core.windows.net/env.yml"
    adapter.write_text(uri, "timezone: UTC")

    assert adapter.exists(uri)
    assert adapter.read_text(uri) == "timezone: UTC"
    assert not adapter.exists(
        "abfss://configs@contoso.dfs.core.windows.net/missing.yml"
    )


def test_adls_storage_adapter_managed_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeCredential:
        def __init__(self, *, client_id: str | None = None) -> None:
            captured["client_id"] = client_id

    class FakeClient:
        def __init__(self, *, account_url: str, credential: object | None = None) -> None:
            captured["account_url"] = account_url
            captured["credential"] = credential

        def get_file_system_client(self, filesystem: str) -> None:  # pragma: no cover
            raise AssertionError("No filesystem interactions expected")

    monkeypatch.setattr("mvp_config_driven.adapters.azure.storage_adls.ManagedIdentityCredential", FakeCredential)
    monkeypatch.setattr("mvp_config_driven.adapters.azure.storage_adls.DataLakeServiceClient", FakeClient)

    AdlsStorageAdapter(
        account_name="contoso",
        auth="managed_identity",
        managed_identity_client_id="client-123",
    )

    assert captured["account_url"] == "https://contoso.dfs.core.windows.net"
    assert isinstance(captured["credential"], FakeCredential)
    assert captured["client_id"] == "client-123"


def test_adls_storage_adapter_sas_token(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, *, account_url: str, credential: object | None = None) -> None:
            captured["account_url"] = account_url
            captured["credential"] = credential

        def get_file_system_client(self, filesystem: str) -> None:  # pragma: no cover
            raise AssertionError("Unexpected filesystem access")

    monkeypatch.setattr("mvp_config_driven.adapters.azure.storage_adls.DataLakeServiceClient", FakeClient)

    AdlsStorageAdapter(account_name="contoso", auth="sas", sas_token="?sig=abc")

    assert captured["account_url"] == "https://contoso.dfs.core.windows.net"
    assert captured["credential"] == "?sig=abc"


def test_keyvault_secrets_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSecret:
        def __init__(self, value: str) -> None:
            self.value = value

    class FakeClient:
        def __init__(self, vault_url: str, credential: object | None = None) -> None:
            self._vault_url = vault_url
            self._credential = credential
            self._values = {"token": FakeSecret("value")}

        def get_secret(self, key: str) -> FakeSecret:
            if key not in self._values:
                raise KeyVaultResourceNotFoundError()
            return self._values[key]

    monkeypatch.setattr(
        "mvp_config_driven.adapters.azure.secret_keyvault.SecretClient",
        FakeClient,
    )

    adapter = KeyVaultSecretsAdapter(vault_url="https://vault.local", credential=None)

    assert adapter.get_secret("token") == "value"
    assert adapter.get_secret("missing") is None


def test_keyvault_secrets_adapter_managed_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeCredential:
        def __init__(self, *, client_id: str | None = None) -> None:
            captured["client_id"] = client_id

    class FakeClient:
        def __init__(self, vault_url: str, credential: object | None = None) -> None:
            captured["vault_url"] = vault_url
            captured["credential"] = credential

        def get_secret(self, key: str) -> None:  # pragma: no cover
            raise KeyVaultResourceNotFoundError()

    monkeypatch.setattr(
        "mvp_config_driven.adapters.azure.secret_keyvault.ManagedIdentityCredential",
        FakeCredential,
    )
    monkeypatch.setattr(
        "mvp_config_driven.adapters.azure.secret_keyvault.SecretClient",
        FakeClient,
    )

    KeyVaultSecretsAdapter(
        vault_url="https://vault.local",
        auth="managed_identity",
        managed_identity_client_id="abc",
    )

    assert captured["vault_url"] == "https://vault.local"
    assert isinstance(captured["credential"], FakeCredential)
    assert captured["client_id"] == "abc"


def test_databricks_job_backend_builds_commands() -> None:
    backend = DatabricksJobBackend(
        host="https://adb.local",
        token="token",
        default_job_id="123",
        default_pipeline_id="pipeline-1",
    )

    command = backend.build_run_now_command(
        notebook_params={"layer": "raw"},
        python_params=["--layer", "raw"],
    )

    assert command[:5] == ["databricks", "--host", "https://adb.local", "--token", "token"]
    assert command[5:7] == ["jobs", "run-now"]
    payload = json.loads(command[-1])
    assert payload["job_id"] == "123"
    assert payload["notebook_params"] == {"layer": "raw"}
    assert payload["python_params"] == ["--layer", "raw"]

    pipeline_cmd = backend.build_pipeline_start_command(full_refresh=True)
    assert pipeline_cmd[:5] == ["databricks", "--host", "https://adb.local", "--token", "token"]
    assert pipeline_cmd[5:] == ["pipelines", "start", "--pipeline-id", "pipeline-1", "--full-refresh"]


def test_build_adapters_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    import mvp_config_driven.adapters as adapters_module

    monkeypatch.setattr(adapters_module, "_FACTORIES", {})
    monkeypatch.setattr(adapters_module, "_INITIALIZED", False)

    class FakeStorageClient:
        def __init__(self, *, account_url: str, credential: object | None = None) -> None:
            self.account_url = account_url
            self.credential = credential

        def get_file_system_client(self, filesystem: str) -> FakeFileSystemClient:
            return FakeFileSystemClient(filesystem, {})

    class FakeSecretClient:
        def __init__(self, vault_url: str, credential: object | None = None) -> None:
            self.vault_url = vault_url
            self.credential = credential

        def get_secret(self, key: str) -> None:
            raise KeyVaultResourceNotFoundError()

    monkeypatch.setattr(
        "mvp_config_driven.adapters.azure.storage_adls.DataLakeServiceClient",
        FakeStorageClient,
    )
    monkeypatch.setattr(
        "mvp_config_driven.adapters.azure.secret_keyvault.SecretClient",
        FakeSecretClient,
    )

    adapters = adapters_module.build_adapters(
        platform="azure",
        config={
            "storage": {"account_name": "contoso", "auth": "sas", "sas_token": "?sig=abc"},
            "secrets": {"vault_url": "https://vault.local"},
            "job_backend": {
                "cli_path": "dbx",
                "host": "https://adb.local",
                "token": "secret",
                "job_id": "42",
            },
        },
    )

    assert isinstance(adapters["storage"], AdlsStorageAdapter)
    assert isinstance(adapters["secrets"], KeyVaultSecretsAdapter)
    assert isinstance(adapters["job_backend"], DatabricksJobBackend)
    assert adapters["job_backend"].build_pipeline_start_command(pipeline_id="p")[0] == "dbx"
