import pytest

from datacore.core.incremental import prepare_incremental


def test_handle_incremental_merge_storage(tmp_path, spark):
    path = tmp_path / "dataset"
    existing = spark.createDataFrame(
        [(1, "old", "2024-01-01 00:00:00"), (2, "keep", "2024-01-02 00:00:00")],
        ["id", "value", "_ingestion_ts"],
    )
    existing.write.mode("overwrite").parquet(str(path))

    new_df = spark.createDataFrame(
        [(1, "new", "2024-02-01 00:00:00"), (3, "fresh", "2024-02-02 00:00:00")],
        ["id", "value", "_ingestion_ts"],
    )

    _, _, handled = prepare_incremental(
        new_df,
        {"type": "storage", "uri": str(path), "format": "parquet"},
        {"mode": "merge", "keys": ["id"], "order_by": ["_ingestion_ts DESC"]},
    )

    assert handled is True
    result = spark.read.parquet(str(path)).orderBy("id").collect()
    assert [row.value for row in result] == ["new", "keep", "fresh"]


def test_handle_incremental_non_merge_returns_false(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "value"])
    _, sink, handled = prepare_incremental(
        df,
        {"type": "storage", "uri": "/tmp/x"},
        {"mode": "append"},
    )
    assert handled is False
    assert sink.get("mode", "append") == "append"


def test_prepare_incremental_full_overwrite(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "value"])
    result_df, sink, handled = prepare_incremental(
        df,
        {"type": "storage", "uri": "/tmp/x"},
        {"mode": "full"},
    )
    assert handled is False
    assert sink["mode"] == "overwrite"
    assert result_df is df


class _FakePlatform:
    def merge_into_warehouse(self, df, sink, keys, order_by):
        self.called = (df.count(), tuple(keys), tuple(order_by))
        return True


def test_merge_with_platform_short_circuit(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "value"])
    platform = _FakePlatform()
    _, _, handled = prepare_incremental(
        df,
        {"type": "warehouse", "engine": "postgres", "url": "jdbc://", "table": "public.t"},
        {"mode": "merge", "keys": ["id"], "order_by": ["id DESC"]},
        platform=platform,
    )
    assert handled is True
    assert platform.called[1] == ("id",)


def test_merge_jdbc_fallback(monkeypatch, spark):
    df = spark.createDataFrame([(1, "A")], ["id", "value"])
    calls = {}

    def fake_read(spark_session, sink_conf):
        return spark.createDataFrame([(2, "B")], ["id", "value"])

    def fake_write(dataframe, sink_conf):
        calls["write"] = dataframe.orderBy("id").collect()

    monkeypatch.setattr("datacore.core.incremental.jdbc.read", fake_read)
    monkeypatch.setattr("datacore.core.incremental.jdbc.write", fake_write)

    _, _, handled = prepare_incremental(
        df,
        {"type": "warehouse", "url": "jdbc://", "table": "public.t"},
        {"mode": "merge", "keys": ["id"], "order_by": ["id DESC"]},
    )

    assert handled is True
    assert [row.value for row in calls["write"]] == ["A", "B"]


def test_merge_requires_keys(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "value"])
    with pytest.raises(ValueError):
        prepare_incremental(df, {"type": "storage", "uri": "out"}, {"mode": "merge"})


def test_merge_invalid_sink(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "value"])
    with pytest.raises(ValueError):
        prepare_incremental(
            df,
            {"type": "unknown"},
            {"mode": "merge", "keys": ["id"]},
        )
