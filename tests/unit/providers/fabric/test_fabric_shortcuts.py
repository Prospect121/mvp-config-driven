"""Pruebas unitarias para la gestión de shortcuts en Fabric."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pytest

from datacore.providers.fabric.shortcuts import ensure_shortcut, fingerprint


class DummyResponse:
    def __init__(self, payload: Dict[str, Any] | None, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.content = b"{}" if payload is not None else b""

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


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


def _base_url(workspace: str) -> str:
    return f"https://api.fabric.microsoft.com/v1/workspaces/{workspace}/shortcuts"


@pytest.mark.parametrize("existing_fp", [None, "different"])
def test_ensure_shortcut_creates_or_updates(existing_fp):
    workspace = "wk-1"
    shortcut_cfg = {
        "name": "sales_onelake",
        "type": "adls",
        "target": "https://storage",
        "labels": ["bronze"],
    }
    desired_fp = fingerprint(shortcut_cfg)
    existing = {
        "id": "shortcut-1",
        "name": "sales_onelake",
        "properties": {"fingerprint": existing_fp},
    }
    responses = [
        ("GET", DummyResponse({"value": [] if existing_fp is None else [existing]})),
    ]
    if existing_fp is None:
        responses.append(("POST", DummyResponse({"id": "shortcut-1"})))
    else:
        responses.append(("PATCH", DummyResponse({"id": "shortcut-1"})))
    env = DummyEnv(workspace, responses)
    ensure_shortcut(env, shortcut_cfg)
    assert env.calls[0][0] == "GET"
    if existing_fp is None:
        method, url, kwargs = env.calls[1]
        assert method == "POST"
        assert url == _base_url(workspace)
        payload = kwargs.get("json")
        assert payload["name"] == "sales_onelake"
        assert payload["properties"]["fingerprint"] == desired_fp
    else:
        method, url, kwargs = env.calls[1]
        assert method == "PATCH"
        payload = kwargs.get("json")
        assert payload["properties"]["fingerprint"] == desired_fp


def test_ensure_shortcut_noop_when_same_fingerprint():
    workspace = "wk-1"
    shortcut_cfg = {
        "name": "sales_onelake",
        "type": "adls",
        "target": "https://storage",
    }
    desired_fp = fingerprint(shortcut_cfg)
    existing = {
        "id": "shortcut-1",
        "name": "sales_onelake",
        "properties": {"fingerprint": desired_fp},
    }
    env = DummyEnv("wk-1", [("GET", DummyResponse({"value": [existing]}))])
    ensure_shortcut(env, shortcut_cfg)
    assert len(env.calls) == 1
