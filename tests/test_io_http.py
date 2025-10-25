import json
from typing import Any, Dict, List

import pytest

from datacore.io.http import HttpFetchMetrics, fetch_json_to_df


class DummyResponse:
    def __init__(self, payload: Dict[str, Any], headers: Dict[str, Any] | None = None):
        self._payload = payload
        self.headers = headers or {"Content-Type": "application/json"}

    def json(self) -> Dict[str, Any]:
        return self._payload

    @property
    def text(self) -> str:
        return json.dumps(self._payload)

    def raise_for_status(self) -> None:
        return None


class DummySession:
    def __init__(self, responses: List["DummyResponse"]):
        self._responses = responses
        self.verify = True
        self.calls = []

    def request(self, method: str, url: str, **kwargs: Any):
        self.calls.append((method, url, kwargs))
        if not self._responses:
            raise RuntimeError("No more responses configured")
        return self._responses.pop(0)


def test_fetch_json_to_df_param_pagination(monkeypatch):
    responses = [
        DummyResponse({"data": [{"id": 1, "updated_at": "2024-01-01T00:00:00Z"}]}),
        DummyResponse({"data": [{"id": 2, "updated_at": "2024-01-02T00:00:00Z"}]}),
    ]
    dummy_session = DummySession(responses)

    monkeypatch.setattr("datacore.io.http._RetryableSession", lambda *a, **k: dummy_session)

    cfg = {
        "url": "https://api.example.com/transactions",
        "params": {"page": 1, "page_size": 1},
        "pagination": {"strategy": "param_increment", "param": "page", "max_pages": 5},
        "incremental": {
            "watermark": {
                "field": "updated_at",
                "value": "2024-01-01T00:00:00Z",
                "state_id": "raw_fin_transactions_http",
            }
        },
    }

    df, metrics = fetch_json_to_df(cfg)

    assert isinstance(metrics, HttpFetchMetrics)
    assert metrics.pages == 2
    assert metrics.records == 2
    assert metrics.watermark == "2024-01-02T00:00:00Z"
    assert metrics.state_id == "raw_fin_transactions_http"

    if hasattr(df, "toPandas"):
        pdf = df.toPandas() if hasattr(df, "toPandas") else df
        assert len(pdf) == 2
    elif hasattr(df, "shape"):
        assert df.shape[0] == 2
    else:  # pragma: no cover - defensive fallback
        pytest.skip("Unsupported dataframe backend in tests")


def test_fetch_json_to_df_cursor(monkeypatch):
    responses = [
        DummyResponse({"results": [{"id": 1}], "next_cursor": "abc"}),
        DummyResponse({"results": [{"id": 2}]}, headers={"Content-Type": "application/json"}),
    ]
    dummy_session = DummySession(responses)
    monkeypatch.setattr("datacore.io.http._RetryableSession", lambda *a, **k: dummy_session)

    cfg = {
        "url": "https://api.example.com/transactions",
        "pagination": {"strategy": "cursor", "cursor_param": "cursor", "cursor_field": "next_cursor"},
    }

    df, metrics = fetch_json_to_df(cfg)
    assert metrics.pages == 2
    assert metrics.records == 2
    assert metrics.watermark is None

    assert len(dummy_session.calls) == 2


def test_http_rate_limit_respected(monkeypatch):
    responses = [
        DummyResponse({"data": [{"id": i}]}) for i in range(6)
    ]
    dummy_session = DummySession(list(responses))

    monkeypatch.setattr("datacore.io.http._RetryableSession", lambda *a, **k: dummy_session)

    current_time = {"value": 0.0}

    def fake_monotonic() -> float:
        return current_time["value"]

    slept: List[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        current_time["value"] += seconds

    monkeypatch.setattr("datacore.io.http.time.monotonic", fake_monotonic)
    monkeypatch.setattr("datacore.io.http.time.sleep", fake_sleep)

    cfg = {
        "url": "https://api.example.com/transactions",
        "pagination": {"strategy": "param_increment", "max_pages": 6},
        "rate_limit": {"requests_per_minute": 6},
    }

    fetch_json_to_df(cfg)

    assert len(dummy_session.calls) == 6
    assert current_time["value"] <= 60.0
    assert sum(slept) == pytest.approx(50.0, rel=1e-6)


def test_http_bearer_env_auth(monkeypatch):
    dummy_session = DummySession([DummyResponse({"data": []})])
    monkeypatch.setattr("datacore.io.http._RetryableSession", lambda *a, **k: dummy_session)
    monkeypatch.setenv("FINBANK_TOKEN", "secret-token")

    cfg = {
        "url": "https://api.example.com/transactions",
        "auth": {"type": "bearer_env", "env": "FINBANK_TOKEN"},
    }

    fetch_json_to_df(cfg)

    _, _, kwargs = dummy_session.calls[0]
    assert kwargs["headers"]["Authorization"] == "Bearer secret-token"


def test_http_basic_env_auth(monkeypatch):
    dummy_session = DummySession([DummyResponse({"data": []})])
    monkeypatch.setattr("datacore.io.http._RetryableSession", lambda *a, **k: dummy_session)
    monkeypatch.setenv("HTTP_USER", "svc-user")
    monkeypatch.setenv("HTTP_PASS", "svc-pass")

    cfg = {
        "url": "https://api.example.com/transactions",
        "auth": {"type": "basic_env", "username_env": "HTTP_USER", "password_env": "HTTP_PASS"},
    }

    fetch_json_to_df(cfg)

    _, _, kwargs = dummy_session.calls[0]
    assert kwargs["auth"] == ("svc-user", "svc-pass")


def test_http_api_key_header_auth(monkeypatch):
    dummy_session = DummySession([DummyResponse({"data": []})])
    monkeypatch.setattr("datacore.io.http._RetryableSession", lambda *a, **k: dummy_session)
    monkeypatch.setenv("API_TOKEN", "key123")

    cfg = {
        "url": "https://api.example.com/transactions",
        "auth": {"type": "api_key_header", "header": "X-API-Key", "env": "API_TOKEN"},
    }

    fetch_json_to_df(cfg)

    _, _, kwargs = dummy_session.calls[0]
    assert kwargs["headers"]["X-API-Key"] == "key123"


def test_http_oauth_client_credentials(monkeypatch):
    dummy_session = DummySession([DummyResponse({"data": []})])
    monkeypatch.setattr("datacore.io.http._RetryableSession", lambda *a, **k: dummy_session)
    monkeypatch.setenv("OAUTH_CLIENT", "client-id")
    monkeypatch.setenv("OAUTH_SECRET", "client-secret")

    class DummyTokenResponse:
        def __init__(self) -> None:
            self._payload = {"access_token": "token-value", "token_type": "Bearer"}

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    captured: Dict[str, Any] = {}

    def fake_post(url: str, data: Dict[str, Any], auth: Any, timeout: float):
        captured["url"] = url
        captured["data"] = dict(data)
        captured["auth"] = auth
        captured["timeout"] = timeout
        return DummyTokenResponse()

    monkeypatch.setattr("datacore.io.http.requests.post", fake_post)

    cfg = {
        "url": "https://api.example.com/transactions",
        "auth": {
            "type": "oauth2_client_credentials",
            "token_url": "https://login.example.com/token",
            "client_id_env": "OAUTH_CLIENT",
            "client_secret_env": "OAUTH_SECRET",
            "scope": "transactions:read",
        },
    }

    fetch_json_to_df(cfg)

    assert captured["url"] == "https://login.example.com/token"
    assert captured["auth"] == ("client-id", "client-secret")
    assert captured["data"]["scope"] == "transactions:read"
    _, _, kwargs = dummy_session.calls[0]
    assert kwargs["headers"]["Authorization"] == "Bearer token-value"
