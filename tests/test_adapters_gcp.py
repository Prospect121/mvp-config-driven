import json
import os
import sys
import types
from pathlib import Path
from typing import Any

import pytest


if "pydantic" not in sys.modules:
    fake_pydantic = types.ModuleType("pydantic")

    class _BaseModel:
        model_config = {}

        @classmethod
        def model_validate(cls, value: object) -> object:
            return value

        def model_dump(self, **_: object) -> dict[str, Any]:
            return {}

    def _field(*, default: object = None, default_factory=None, **__: object) -> object:
        if default_factory is not None:
            return default_factory()
        return default

    def _config_dict(**kwargs: object) -> dict[str, object]:
        return dict(kwargs)

    fake_pydantic.BaseModel = _BaseModel
    fake_pydantic.Field = _field
    fake_pydantic.ConfigDict = _config_dict
    sys.modules["pydantic"] = fake_pydantic


# ---------------------------------------------------------------------------
# Lightweight stubs for optional google cloud dependencies used by the adapters
# ---------------------------------------------------------------------------

if "gcsfs" not in sys.modules:
    fake_gcsfs = types.ModuleType("gcsfs")
    fake_gcsfs._STORE: dict[tuple[str, str], bytes] = {}

    class _FakeGCSHandle:
        def __init__(self, store: dict[tuple[str, str], bytes], uri: str, mode: str) -> None:
            self._store = store
            self._uri = uri
            self._mode = mode

        def __enter__(self) -> "_FakeGCSHandle":
            return self

        def __exit__(self, *exc: object) -> None:  # pragma: no cover - context manager protocol
            return None

        def read(self) -> bytes | str:
            bucket, blob = self._split(self._uri)
            data = self._store.get((bucket, blob), b"")
            if "b" in self._mode:
                return data
            return data.decode("utf-8")

        def write(self, data: bytes | str) -> None:  # pragma: no cover - unused helper
            bucket, blob = self._split(self._uri)
            if isinstance(data, str):
                data = data.encode("utf-8")
            self._store[(bucket, blob)] = data

        @staticmethod
        def _split(uri: str) -> tuple[str, str]:
            if not uri.startswith("gs://"):
                raise ValueError(uri)
            path = uri[5:]
            bucket, _, blob = path.partition("/")
            return bucket, blob

    class FakeGCSFileSystem:
        def __init__(self, **options: object) -> None:
            self.options = options

        def open(self, uri: str, mode: str = "rb") -> _FakeGCSHandle:
            return _FakeGCSHandle(fake_gcsfs._STORE, uri, mode)

    fake_gcsfs.GCSFileSystem = FakeGCSFileSystem
    sys.modules["gcsfs"] = fake_gcsfs

if "google" not in sys.modules:
    sys.modules["google"] = types.ModuleType("google")

if "google.cloud" not in sys.modules:
    sys.modules["google.cloud"] = types.ModuleType("google.cloud")

if "google.api_core" not in sys.modules:
    api_core = types.ModuleType("google.api_core")
    exceptions_module = types.ModuleType("google.api_core.exceptions")

    class NotFound(Exception):
        pass

    exceptions_module.NotFound = NotFound
    api_core.exceptions = exceptions_module
    sys.modules["google.api_core"] = api_core
    sys.modules["google.api_core.exceptions"] = exceptions_module
else:
    exceptions_module = sys.modules.setdefault(
        "google.api_core.exceptions", types.ModuleType("google.api_core.exceptions")
    )
    if not hasattr(exceptions_module, "NotFound"):
        exceptions_module.NotFound = type("NotFound", (Exception,), {})

if "google.cloud.storage" not in sys.modules:
    storage_module = types.ModuleType("google.cloud.storage")
    storage_module._STORE: dict[tuple[str, str], bytes] = {}

    class FakeBlob:
        def __init__(self, bucket: "FakeBucket", name: str) -> None:
            self._bucket = bucket
            self._name = name

        def upload_from_string(self, data: bytes) -> None:
            storage_module._STORE[(self._bucket.name, self._name)] = data

        def download_as_bytes(self) -> bytes:
            return storage_module._STORE[(self._bucket.name, self._name)]

        def exists(self, client: object | None = None) -> bool:  # pragma: no cover - simple proxy
            return (self._bucket.name, self._name) in storage_module._STORE

    class FakeBucket:
        def __init__(self, name: str) -> None:
            self.name = name

        def blob(self, name: str) -> FakeBlob:
            return FakeBlob(self, name)

    class FakeClient:
        def __init__(self, project: str | None = None, credentials: object | None = None) -> None:
            self.project = project
            self.credentials = credentials

        def bucket(self, name: str) -> FakeBucket:
            return FakeBucket(name)

    storage_module.Client = FakeClient
    storage_module.Blob = FakeBlob
    sys.modules["google.cloud.storage"] = storage_module
else:
    storage_module = sys.modules["google.cloud.storage"]

if "google.cloud.secretmanager" not in sys.modules:
    secretmanager_module = types.ModuleType("google.cloud.secretmanager")
    secretmanager_module._STORE: dict[str, bytes] = {}

    class FakePayload:
        def __init__(self, data: bytes) -> None:
            self.data = data

    class FakeSecretResponse:
        def __init__(self, data: bytes) -> None:
            self.payload = FakePayload(data)

    class FakeSecretManagerServiceClient:
        def __init__(self, credentials: object | None = None) -> None:
            self.credentials = credentials
            self.calls: list[str] = []

        def access_secret_version(self, *, name: str) -> FakeSecretResponse:
            self.calls.append(name)
            if name not in secretmanager_module._STORE:
                raise exceptions_module.NotFound()
            return FakeSecretResponse(secretmanager_module._STORE[name])

    secretmanager_module.SecretManagerServiceClient = FakeSecretManagerServiceClient
    sys.modules["google.cloud.secretmanager"] = secretmanager_module
else:
    secretmanager_module = sys.modules["google.cloud.secretmanager"]

if "google.oauth2" not in sys.modules:
    oauth_module = types.ModuleType("google.oauth2")
    sys.modules["google.oauth2"] = oauth_module
else:
    oauth_module = sys.modules["google.oauth2"]

if "google.oauth2.service_account" not in sys.modules:
    service_account_module = types.ModuleType("google.oauth2.service_account")

    class FakeServiceAccountCredentials:
        def __init__(self, info: dict[str, Any]) -> None:
            self.info = dict(info)

        @classmethod
        def from_service_account_info(cls, info: dict[str, Any]) -> "FakeServiceAccountCredentials":
            return cls(info)

    service_account_module.Credentials = FakeServiceAccountCredentials
    sys.modules["google.oauth2.service_account"] = service_account_module
    oauth_module.service_account = service_account_module


from mvp_config_driven.adapters.gcp.job_dataproc import DataprocJobBackend
from mvp_config_driven.adapters.gcp.secret_manager import SecretManagerAdapter
from mvp_config_driven.adapters.gcp.storage_gcs import GCSStorageAdapter


class DummySecretProvider:
    def __init__(self, secrets: dict[str, str]) -> None:
        self._secrets = secrets
        self.calls: list[str] = []

    def get_secret(self, key: str) -> str | None:
        self.calls.append(key)
        return self._secrets.get(key)


def _reset_stores() -> None:
    shared_store: dict[tuple[str, str], bytes] = {}
    sys.modules["gcsfs"]._STORE = shared_store
    sys.modules["google.cloud.storage"]._STORE = shared_store
    sys.modules["google.cloud.secretmanager"]._STORE.clear()


def test_gcs_storage_adapter_roundtrip() -> None:
    _reset_stores()
    store = sys.modules["google.cloud.storage"]._STORE
    provider = DummySecretProvider({})
    adapter = GCSStorageAdapter(
        project="demo",
        secret_provider=provider,
        gcsfs_options={},
        client=storage_module.Client(project="demo"),
    )

    uri = "gs://configs/env.yml"
    adapter.write_text(uri, "timezone: UTC")

    assert adapter.exists(uri)
    assert adapter.read_text(uri) == "timezone: UTC"
    assert not adapter.exists("gs://configs/missing.yml")


def test_gcs_storage_adapter_resolves_credentials_secret() -> None:
    _reset_stores()
    payload = json.dumps({"type": "service_account", "project_id": "demo"})
    provider = DummySecretProvider({"gcs": payload})

    adapter = GCSStorageAdapter(
        project="demo",
        auth="service_account",
        secret_provider=provider,
        credentials_secret="gcs",
        gcsfs_options={},
        client=storage_module.Client(project="demo"),
    )

    filesystem = adapter._filesystem  # type: ignore[attr-defined]
    assert isinstance(filesystem.options["token"].info, dict)  # type: ignore[index]
    assert provider.calls == ["gcs"]


def test_secret_manager_adapter_returns_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_stores()
    secretmanager_module._STORE[
        "projects/demo/secrets/token/versions/latest"
    ] = b"super-secret"

    client = secretmanager_module.SecretManagerServiceClient()
    adapter = SecretManagerAdapter(project_id="demo", client=client)

    assert adapter.get_secret("token") == "super-secret"
    assert adapter.get_secret("projects/demo/secrets/token/versions/latest") == "super-secret"
    assert adapter.get_secret("missing") is None
    # cache hit
    assert adapter.get_secret("token") == "super-secret"
    assert client.calls.count("projects/demo/secrets/token/versions/latest") == 2


def test_dataproc_job_backend_builds_command(tmp_path: Path) -> None:
    _reset_stores()
    secrets = DummySecretProvider({
        "runner": json.dumps({"type": "service_account", "client_email": "svc@demo"})
    })

    backend = DataprocJobBackend(
        default_project="demo",
        default_region="us-central1",
        default_service_account="svc@demo",
        secret_provider=secrets,
        credentials_secret="runner",
        workload_identity=False,
    )

    command, env = backend.build_batches_submit_command(
        batch_id="pipeline-1",
        main_python_file_uri="gs://scripts/entry.py",
        jars=["gs://libs/pipeline.whl"],
        properties={"spark.executor.instances": "2"},
        labels={"env": "prod", "team": "data"},
        args=["--layer", "raw"],
    )

    assert command[:6] == [
        "gcloud",
        "dataproc",
        "batches",
        "submit",
        "pyspark",
        "gs://scripts/entry.py",
    ]
    assert "--batch" in command
    assert "--region" in command
    assert "--project" in command
    assert "--service-account" in command
    assert command[-3:] == ["--", "--layer", "raw"]

    cred_path = Path(env["GOOGLE_APPLICATION_CREDENTIALS"])
    assert cred_path.exists()
    data = cred_path.read_text(encoding="utf-8")
    assert json.loads(data)["client_email"] == "svc@demo"
    os.unlink(cred_path)
    assert secrets.calls == ["runner"]


def test_build_adapters_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_stores()
    secretmanager_module._STORE[
        "projects/demo/secrets/gcs/versions/latest"
    ] = json.dumps({"type": "service_account", "project_id": "demo"}).encode("utf-8")
    secretmanager_module._STORE[
        "projects/demo/secrets/runner/versions/latest"
    ] = json.dumps({"type": "service_account", "client_email": "svc@demo"}).encode("utf-8")

    import mvp_config_driven.adapters as adapters_module

    monkeypatch.setattr(adapters_module, "_FACTORIES", {})
    monkeypatch.setattr(adapters_module, "_INITIALIZED", False)

    adapters = adapters_module.build_adapters(
        platform="gcp",
        config={
            "secrets": {"project_id": "demo"},
            "storage": {"project": "demo", "auth": "service_account", "credentials_secret": "gcs"},
            "job_backend": {
                "project": "demo",
                "region": "us-central1",
                "service_account": "svc@demo",
                "credentials_secret": "runner",
            },
        },
    )

    from mvp_config_driven.adapters.gcp.job_dataproc import DataprocJobBackend as BackendCls
    from mvp_config_driven.adapters.gcp.secret_manager import SecretManagerAdapter as SecretsCls
    from mvp_config_driven.adapters.gcp.storage_gcs import GCSStorageAdapter as StorageCls

    assert isinstance(adapters["secrets"], SecretsCls)
    assert isinstance(adapters["storage"], StorageCls)
    assert isinstance(adapters["job_backend"], BackendCls)

    command, env = adapters["job_backend"].build_batches_submit_command(
        batch_id="run-1", main_python_file_uri="gs://scripts/entry.py", args=["--layer", "raw"]
    )
    assert command[0] == "gcloud"
    cred_path = Path(env.get("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/missing"))
    if cred_path.exists():
        os.unlink(cred_path)
