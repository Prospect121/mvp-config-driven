from __future__ import annotations

import pytest

from datacore.providers.fabric.env import FabricEnv


def test_env_resolves_placeholders(monkeypatch):
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "wk-1")
    env = FabricEnv({"workspace_id": "${FABRIC_WORKSPACE_ID}"})
    assert env.workspace_id == "wk-1"


def test_env_rejects_unknown_placeholder(monkeypatch):
    with pytest.raises(RuntimeError):
        FabricEnv({"workspace_id": "${NOT_ALLOWED}"})
