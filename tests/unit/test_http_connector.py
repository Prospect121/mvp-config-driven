from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from datacore.connectors import http


@dataclass
class _FakeResponse:
    payload: dict[str, Any]
    status_code: int = 200
    headers: dict[str, str] | None = None

    def json(self) -> dict[str, Any]:
        return self.payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"status {self.status_code}")

    @property
    def text(self) -> str:
        return "ok"


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]):
        self._responses = responses
        self.calls: list[dict[str, Any]] = []
        self.auth = None

    def request(self, method: str, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self._responses:
            raise RuntimeError("no more responses")
        return self._responses.pop(0)


@pytest.mark.parametrize("strategy", ["page", "cursor", "link"])
def test_fetch_pages_strategies(monkeypatch, strategy: str) -> None:
    if strategy == "page":
        responses = [
            _FakeResponse({"data": [1]}),
            _FakeResponse({"data": [2]}),
        ]
        pagination = {"strategy": "page", "param": "page", "max_pages": 2}
    elif strategy == "cursor":
        responses = [
            _FakeResponse({"items": [1], "next": "cursor-2"}),
            _FakeResponse({"items": [2], "next": None}),
        ]
        pagination = {"strategy": "cursor", "param": "cursor", "cursor_path": "$.next"}
    else:
        responses = [
            _FakeResponse({"items": [1]}, headers={"Link": "<http://example.com?page=2>; rel=\"next\""}),
            _FakeResponse({"items": [2]}, headers={}),
        ]
        pagination = {"strategy": "link", "max_pages": 5}

    fake_session = _FakeSession(responses)
    monkeypatch.setattr(http.requests, "Session", lambda: fake_session)

    pages = list(
        http.fetch_pages(
            {
                "url": "https://api.example.com/orders",
                "method": "GET",
                "auth": {"type": "bearer", "token": "XYZ"},
                "pagination": pagination,
                "retry": {"max_attempts": 1},
            }
        )
    )

    assert len(pages) == 2
    assert fake_session.calls[0]["headers"]["Authorization"] == "Bearer XYZ"
    if strategy == "page":
        assert fake_session.calls[0]["params"]["page"] == 1
        assert fake_session.calls[1]["params"]["page"] == 2
    elif strategy == "cursor":
        assert fake_session.calls[1]["params"]["cursor"] == "cursor-2"
    else:
        assert fake_session.calls[1]["url"].endswith("page=2")


def test_fetch_pages_basic_auth(monkeypatch) -> None:
    responses = [_FakeResponse({"items": []})]
    session = _FakeSession(responses)
    monkeypatch.setattr(http.requests, "Session", lambda: session)

    list(
        http.fetch_pages(
            {
                "url": "https://api.example.com/orders",
                "method": "GET",
                "auth": {"type": "basic", "username": "user", "password": "pass"},
            }
        )
    )

    assert session.auth == ("user", "pass")
