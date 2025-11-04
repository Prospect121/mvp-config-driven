from datacore.connectors.api import graphql, rest


class _DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):  # type: ignore[no-untyped-def]
        return None

    def json(self):  # type: ignore[no-untyped-def]
        return self._payload


def test_rest_fetch_pages(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_get(url, headers=None, params=None, timeout=None):  # type: ignore[no-untyped-def]
        calls.append({"headers": headers, "params": params})
        return _DummyResponse({"items": []})

    monkeypatch.setattr(rest.requests, "get", fake_get)
    monkeypatch.setattr(rest.time, "sleep", lambda *_: None)

    list(
        rest.fetch_pages(
            {"url": "https://api.example.com", "bearer_token": "abc", "max_pages": 2}
        )
    )

    assert len(calls) == 2
    assert calls[0]["headers"]["Authorization"] == "Bearer abc"
    assert calls[0]["params"]["page"] == 1
    assert calls[1]["params"]["page"] == 2


def test_graphql_execute_query(monkeypatch):
    captured: dict[str, object] = {}

    def fake_post(url, json=None, headers=None, timeout=None):  # type: ignore[no-untyped-def]
        captured.update({"url": url, "json": json, "headers": headers})
        return _DummyResponse({"data": {"ok": True}})

    monkeypatch.setattr(graphql.requests, "post", fake_post)

    response = graphql.execute_query(
        {
            "url": "https://graphql.example.com",
            "query": "query($id: ID!){ item(id: $id) { id } }",
            "variables": {"id": 1},
            "headers": {"X-Token": "secret"},
        }
    )

    assert response["data"]["ok"] is True
    assert captured["url"] == "https://graphql.example.com"
    assert captured["json"]["variables"] == {"id": 1}
    assert captured["headers"]["X-Token"] == "secret"
