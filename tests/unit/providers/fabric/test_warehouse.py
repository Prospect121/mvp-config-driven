from __future__ import annotations

from typing import Any, Dict, List, Tuple

from datacore.providers.fabric import warehouse


class DummyResponse:
    def __init__(self, payload: Dict[str, Any] | None = None, status_code: int = 200):
        self._payload = payload or {}
        self.status_code = status_code
        self.content = b"{}"

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("error")


class DummyEnv:
    def __init__(self, workspace_id: str, warehouse_id: str, responses: List[Tuple[str, DummyResponse]]):
        self.workspace_id = workspace_id
        self.warehouse_id = warehouse_id
        self._responses = responses
        self.calls: List[Tuple[str, str, Dict[str, Any]]] = []

    def http(self, method: str, url: str, **kwargs):
        expected_method, response = self._responses.pop(0)
        assert expected_method == method
        self.calls.append((method, url, kwargs))
        return response


def test_create_or_update_view_builds_statement():
    env = DummyEnv(
        "wk-1",
        "wh-1",
        [("POST", DummyResponse({"operationId": "op-1"}))],
    )
    result = warehouse.create_or_update_view(
        env,
        "lakehouse://tables/bronze/demo",
        "vw_demo",
        columns=[{"name": "amount", "type": "DECIMAL(18,2)"}],
    )
    assert result["view"] == "vw_demo"
    method, url, kwargs = env.calls[0]
    assert method == "POST"
    statement = kwargs["json"]["statement"]
    assert "CREATE OR ALTER VIEW [vw_demo]" in statement
    assert "CAST([amount] AS DECIMAL(18,2)) AS [amount]" in statement


def test_copy_into_composes_options():
    env = DummyEnv(
        "wk-1",
        "wh-1",
        [("POST", DummyResponse({"status": "Queued"}))],
    )
    warehouse.copy_into(
        env,
        "fact_sales",
        "https://contoso.dfs.core.windows.net/raw/sales",
        options={"max_errors": 5, "pattern": "*.parquet"},
    )
    statement = env.calls[0][2]["json"]["statement"]
    assert "COPY INTO [fact_sales]" in statement
    assert "MAXERRORS 5" in statement
    assert "PATTERN = '*.parquet'" in statement
