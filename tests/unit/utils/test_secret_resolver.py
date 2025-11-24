from __future__ import annotations

import sys
import types

import pytest

from datacore.utils import secrets


def test_resolve_secret_azure(monkeypatch):
    class DummyCredential:
        def __init__(self):
            self.tokens = []

        def get_token(self, scope):
            self.tokens.append(scope)
            return types.SimpleNamespace(token="token")

    class DummySecretClient:
        def __init__(self, vault_url, credential):
            self.vault_url = vault_url
            self.credential = credential

        def get_secret(self, name, version=None):
            return types.SimpleNamespace(value=f"secret-{name}-{version or 'latest'}")

    identity_module = types.SimpleNamespace(DefaultAzureCredential=DummyCredential)
    secrets_module = types.SimpleNamespace(SecretClient=DummySecretClient)
    monkeypatch.setitem(sys.modules, "azure", types.SimpleNamespace(identity=identity_module, keyvault=types.SimpleNamespace(secrets=secrets_module)))
    monkeypatch.setitem(sys.modules, "azure.identity", identity_module)
    monkeypatch.setitem(sys.modules, "azure.keyvault", types.SimpleNamespace(secrets=secrets_module))
    monkeypatch.setitem(sys.modules, "azure.keyvault.secrets", secrets_module)
    value = secrets.resolve_secret("secret://kv/myvault/mysecret")
    assert value == "secret-mysecret-latest"
    for name in ["azure.keyvault.secrets", "azure.keyvault", "azure.identity", "azure"]:
        sys.modules.pop(name, None)


def test_resolve_secret_aws(monkeypatch):
    class DummyClient:
        def get_secret_value(self, SecretId):
            return {"SecretString": "value"}

    boto3_module = types.SimpleNamespace(client=lambda name: DummyClient())
    monkeypatch.setitem(sys.modules, "boto3", boto3_module)
    assert secrets.resolve_secret("secret://sm/mysecret") == "value"
    sys.modules.pop("boto3", None)


def test_resolve_secret_gcp(monkeypatch):
    class DummyPayload:
        def __init__(self, data):
            self.data = data

    class DummyResponse:
        def __init__(self, data):
            self.payload = DummyPayload(data)

    class DummyClient:
        def access_secret_version(self, name):
            return DummyResponse(b"gcp-value")

    secretmanager_module = types.SimpleNamespace(SecretManagerServiceClient=DummyClient)
    monkeypatch.setitem(sys.modules, "google", types.SimpleNamespace(cloud=types.SimpleNamespace(secretmanager=secretmanager_module)))
    monkeypatch.setitem(sys.modules, "google.cloud", types.SimpleNamespace(secretmanager=secretmanager_module))
    monkeypatch.setitem(sys.modules, "google.cloud.secretmanager", secretmanager_module)
    assert secrets.resolve_secret("secret://gcp/project/secret/1") == "gcp-value"
    for name in ["google.cloud.secretmanager", "google.cloud", "google"]:
        sys.modules.pop(name, None)


def test_resolve_secret_invalid_scheme():
    with pytest.raises(ValueError):
        secrets.resolve_secret("http://example")
