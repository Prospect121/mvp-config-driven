"""Pruebas para el orquestador de Fabric."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from datacore.providers.fabric import orchestrator


class DummyResponse:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload
        self.content = b"{}"

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class DummyEnv:
    def __init__(self, workspace_id: str, responses: List[Tuple[str, DummyResponse]]):
        self.workspace_id = workspace_id
        self._responses = responses
        self.calls: List[Tuple[str, str, Dict[str, Any] | None]] = []

    def http(self, method: str, url: str, **kwargs):
        payload = kwargs.get("json")
        self.calls.append((method, url, payload))
        expected_method, response = self._responses.pop(0)
        assert expected_method == method
        return response


def test_ensure_spark_job(monkeypatch):
    env = DummyEnv(
        "wk-1",
        [
            (
                "POST",
                DummyResponse({"id": "item-1", "webUrl": "https://fabric/jobs/1", "name": "job"}),
            )
        ],
    )
    result = orchestrator.ensure_spark_job(env, {"name": "job", "entrypoint": "python foo"})
    assert result == {"id": "item-1", "url": "https://fabric/jobs/1", "name": "job"}
    method, url, payload = env.calls[0]
    assert method == "POST"
    assert payload["type"] == "sparkJobDefinition"


def test_ensure_pipeline(monkeypatch):
    env = DummyEnv(
        "wk-1",
        [
            (
                "POST",
                DummyResponse({"id": "pipeline-1", "webUrl": "https://fabric/pipeline/1", "name": "pipeline"}),
            )
        ],
    )
    result = orchestrator.ensure_pipeline(env, {"name": "pipeline"})
    assert result == {"id": "pipeline-1", "url": "https://fabric/pipeline/1", "name": "pipeline"}
    method, url, payload = env.calls[0]
    assert method == "POST"
    assert payload["type"] == "dataPipeline"
