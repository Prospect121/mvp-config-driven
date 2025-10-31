import pytest

from datacore.utils import secrets


def test_get_secret_env(monkeypatch):
    monkeypatch.setenv("MY_SECRET", "value")
    assert secrets.get_secret("MY_SECRET") == "value"


def test_get_secret_default(monkeypatch):
    monkeypatch.delenv("MISSING_SECRET", raising=False)
    assert secrets.get_secret("MISSING_SECRET", default="fallback") == "fallback"


def test_get_secret_missing(monkeypatch):
    monkeypatch.delenv("ABSENT", raising=False)
    with pytest.raises(secrets.SecretNotFoundError):
        secrets.get_secret("ABSENT")
