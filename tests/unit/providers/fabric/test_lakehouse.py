"""Pruebas para operaciones de Lakehouse en Fabric."""

from __future__ import annotations

import pytest

from datacore.providers.fabric import lakehouse


class DummyWriter:
    def __init__(self):
        self.format_value = None
        self.mode_value = None
        self.options_value = {}
        self.saved_path = None

    def format(self, value):
        self.format_value = value
        return self

    def mode(self, value):
        self.mode_value = value
        return self

    def options(self, **kwargs):
        self.options_value.update(kwargs)
        return self

    def save(self, path):
        self.saved_path = path


class DummyDataFrame:
    def __init__(self):
        self.write = DummyWriter()


class DummySpark:
    def __init__(self):
        self.sql_calls: list[str] = []

    def sql(self, statement: str):
        self.sql_calls.append(statement)


@pytest.fixture
def dummy_spark(monkeypatch):
    spark = DummySpark()
    monkeypatch.setattr(lakehouse, "_spark", lambda: spark)
    return spark


def test_write_delta_invokes_writer(dummy_spark):
    df = DummyDataFrame()
    options = {"mode": "overwrite", "mergeSchema": True, "maxRecordsPerFile": 1000}
    lakehouse.write_delta(None, df, "lakehouse://tables/bronze/demo", options)
    writer = df.write
    assert writer.format_value == "delta"
    assert writer.mode_value == "overwrite"
    assert writer.options_value == {"mergeSchema": "True", "maxRecordsPerFile": "1000"}
    assert writer.saved_path == "lakehouse://tables/bronze/demo"


def test_optimize_delta_generates_sql(dummy_spark):
    lakehouse.optimize_delta(None, "lakehouse://tables/bronze/demo", {"optimize": True, "vacuum_hours": 168})
    assert dummy_spark.sql_calls == [
        "OPTIMIZE delta.`lakehouse://tables/bronze/demo`",
        "VACUUM delta.`lakehouse://tables/bronze/demo` RETAIN 168 HOURS",
    ]


def test_optimize_delta_with_zorder(dummy_spark):
    dummy_spark.sql_calls.clear()
    lakehouse.optimize_delta(
        None,
        "lakehouse://tables/silver/demo",
        {"zorder": ["col_a", "col_b"]},
    )
    assert dummy_spark.sql_calls == [
        "OPTIMIZE delta.`lakehouse://tables/silver/demo` ZORDER BY (col_a, col_b)",
    ]
