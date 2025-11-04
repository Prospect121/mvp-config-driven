from datetime import datetime

import pytest

from datacore.core.cdc import incremental_fetch_jdbc


@pytest.fixture
def jdbc_reader(monkeypatch, spark):
    captured = {}

    def fake_read(_, cfg):
        captured["config"] = cfg
        data = [
            (1, datetime(2024, 1, 1, 12, 0, 0)),
            (2, datetime(2024, 1, 2, 12, 0, 0)),
        ]
        return spark.createDataFrame(data, ["order_id", "updated_at"])

    monkeypatch.setattr("datacore.connectors.db.jdbc.read", fake_read)
    return captured


def test_incremental_fetch_jdbc_timestamp(monkeypatch, spark, jdbc_reader):
    cfg = {"table": "public.orders"}
    cdc = {"mode": "timestamp", "watermark_column": "updated_at"}
    df, state = incremental_fetch_jdbc(spark, cfg, cdc, {"watermark": "2024-01-01T00:00:00"})
    assert df.count() == 2
    assert "query" in jdbc_reader["config"]
    assert state["watermark"].startswith("2024-01-02")


def test_incremental_fetch_jdbc_version(monkeypatch, spark):
    captured = {}

    def fake_read(_, cfg):
        captured["config"] = cfg
        return spark.createDataFrame([(1, 3), (2, 4)], ["order_id", "version"])

    monkeypatch.setattr("datacore.connectors.db.jdbc.read", fake_read)
    cfg = {"table": "public.orders"}
    cdc = {"mode": "version_column", "watermark_column": "version"}
    df, state = incremental_fetch_jdbc(spark, cfg, cdc, {"version": 2})
    assert df.count() == 2
    assert "query" in captured["config"]
    assert state["version"] == "4"
