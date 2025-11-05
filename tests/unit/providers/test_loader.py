"""Pruebas para el cargador dinámico de providers."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from datacore.providers import load_provider


class DummyFabric(ModuleType):
    def __init__(self):
        super().__init__("datacore.providers.fabric.provider")

        class _Provider:
            def __init__(self, cfg):
                self.cfg = cfg

        self.FabricProvider = _Provider


@pytest.fixture(autouse=True)
def cleanup_modules():
    yield
    for key in list(sys.modules):
        if key.startswith("datacore.providers.fabric"):
            sys.modules.pop(key)


def test_load_provider_without_fabric_section():
    assert load_provider({}) is None


def test_load_provider_missing_extra(monkeypatch):
    real_import = __import__

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "datacore.providers.fabric.provider" or (
            name == "datacore.providers.fabric" and "provider" in fromlist
        ):
            raise ImportError("missing")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(sys.modules["builtins"], "__import__", _fake_import)
    cfg = {"providers": {"fabric": {"workspace_id": "1"}}}
    with pytest.raises(RuntimeError) as exc:
        load_provider(cfg)
    assert "pip install .[fabric]" in str(exc.value)


def test_load_provider_success(monkeypatch):
    module = DummyFabric()
    sys.modules[module.__name__] = module
    package = ModuleType("datacore.providers.fabric")
    package.provider = module
    sys.modules["datacore.providers.fabric"] = package
    cfg = {"providers": {"fabric": {"workspace_id": "1"}}}
    provider = load_provider(cfg)
    assert provider.__class__.__name__ == "_Provider"
    assert provider.cfg == {"workspace_id": "1"}
