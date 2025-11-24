"""Pruebas para el orquestador de Fabric."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from datacore.providers.fabric import orchestrator


class DummyResponse:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200):
        self._payload = payload
        self.content = b"{}"
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("error")


class DummyEnv:
    def __init__(self, workspace_id: str, responses: List[Tuple[str, DummyResponse]]):
        self.workspace_id = workspace_id
        self._responses = responses
        self.calls: List[Tuple[str, str, Dict[str, Any]]] = []

    def http(self, method: str, url: str, **kwargs):
        expected_method, response = self._responses.pop(0)
        assert expected_method == method
        self.calls.append((method, url, kwargs))
        return response


def test_ensure_spark_job(monkeypatch):
    env = DummyEnv(
        "wk-1",
        [
            ("GET", DummyResponse({"value": []})),
            (
                "POST",
                DummyResponse({"id": "item-1", "webUrl": "https://fabric/jobs/1", "name": "job"}),
            ),
        ],
    )
    result = orchestrator.ensure_spark_job(env, {"name": "job", "entrypoint": "python foo"})
    assert result == {"id": "item-1", "url": "https://fabric/jobs/1", "name": "job"}
    assert env.calls[0][0] == "GET"
    method, url, kwargs = env.calls[1]
    assert method == "POST"
    assert kwargs["json"]["type"] == "sparkJobDefinition"


def test_ensure_pipeline(monkeypatch):
    env = DummyEnv(
        "wk-1",
        [
            ("GET", DummyResponse({"value": []})),
            (
                "POST",
                DummyResponse({"id": "pipeline-1", "webUrl": "https://fabric/pipeline/1", "name": "pipeline"}),
            ),
        ],
    )
    result = orchestrator.ensure_pipeline(env, {"name": "pipeline"})
    assert result == {"id": "pipeline-1", "url": "https://fabric/pipeline/1", "name": "pipeline"}
    assert env.calls[0][0] == "GET"
    method, url, kwargs = env.calls[1]
    assert method == "POST"
    assert kwargs["json"]["type"] == "dataPipeline"


def test_orchestrator_idempotent_update():
    existing = {
        "id": "item-1",
        "name": "job",
        "type": "sparkJobDefinition",
        "webUrl": "https://fabric/jobs/1",
        "properties": {"fingerprint": "abc"},
    }
    env = DummyEnv(
        "wk-1",
        [
            ("GET", DummyResponse({"value": [existing]})),
            ("PATCH", DummyResponse({"id": "item-1", "webUrl": "https://fabric/jobs/1"})),
        ],
    )
    orchestrator.ensure_spark_job(env, {"name": "job", "entrypoint": "python foo", "parameters": {"x": 1}})
    assert env.calls[1][0] == "PATCH"
