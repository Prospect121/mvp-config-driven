from __future__ import annotations

import sys
import types

import pytest

from datacore.providers.fabric import kql


class DummyEnv:
    def __init__(self):
        self.workspace_id = "wk-1"
        self.eventhouse_id = "https://cluster"

    def get_token(self):
        return "token"


@pytest.fixture
def azure_modules(monkeypatch):
    data_module = types.SimpleNamespace()
    ingest_module = types.SimpleNamespace()

    class DummyClient:
        def __init__(self, builder):
            self.builder = builder
            self.executed = None

        def execute(self, database, statement):
            self.executed = (database, statement)
            return {"database": database, "statement": statement}

    dummy_client = DummyClient("builder")

    def with_provider(cluster, provider):
        assert provider() == "token"
        return "builder"

    data_module.KustoClient = lambda builder: dummy_client
    data_module.KustoConnectionStringBuilder = types.SimpleNamespace(
        with_aad_token_provider=with_provider
    )

    class DummyIngestClient:
        def __init__(self, builder):
            self.builder = builder
            self.calls = []

        def ingest_from_dataframe(self, df, ingestion_properties):
            self.calls.append((df, ingestion_properties))

    ingest_client = DummyIngestClient("builder")

    ingest_module.IngestionProperties = lambda **kwargs: kwargs
    ingest_module.QueuedIngestClient = lambda builder: ingest_client

    monkeypatch.setitem(
        sys.modules,
        "azure",
        types.SimpleNamespace(kusto=types.SimpleNamespace(data=data_module, ingest=ingest_module)),
    )
    monkeypatch.setitem(sys.modules, "azure.kusto", types.SimpleNamespace(data=data_module, ingest=ingest_module))
    monkeypatch.setitem(sys.modules, "azure.kusto.data", data_module)
    monkeypatch.setitem(sys.modules, "azure.kusto.ingest", ingest_module)
    yield dummy_client, ingest_client
    for name in ["azure.kusto.ingest", "azure.kusto.data", "azure.kusto", "azure"]:
        sys.modules.pop(name, None)


def test_kql_read_uses_client(monkeypatch, azure_modules):
    dummy_client, _ = azure_modules
    env = DummyEnv()
    result = kql.kql_read(env, "KQL", {"database": "db"})
    assert result == {"database": "db", "statement": "KQL"}
    assert dummy_client.executed == ("db", "KQL")


def test_kql_ingest(monkeypatch, azure_modules):
    _, ingest_client = azure_modules
    env = DummyEnv()
    df = object()
    kql.kql_ingest(env, "table", df, {"database": "db"})
    assert ingest_client.calls
