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
        self.save_as_table_name = None
        self.mode_calls: list[str] = []

    def format(self, value):
        self.format_value = value
        return self

    def mode(self, value):
        self.mode_value = value
        self.mode_calls.append(value)
        return self

    def options(self, **kwargs):
        self.options_value.update(kwargs)
        return self

    def option(self, key, value):
        self.options_value[key] = value
        return self

    def save(self, path):
        self.saved_path = path

    def saveAsTable(self, table):
        self.save_as_table_name = table


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


def test_write_delta_uses_save_as_table_for_lakehouse_tables(dummy_spark):
    df = DummyDataFrame()
    options = {"mode": "overwrite", "mergeSchema": True, "maxRecordsPerFile": 1000}
    lakehouse.write_delta(
        None,
        df,
        "lakehouse://contoso-lh/tables/dbo/customers_raw",
        options,
    )
    writer = df.write
    assert writer.format_value == "delta"
    assert writer.mode_value == "overwrite"
    assert writer.options_value == {
        "mergeSchema": "True",
        "maxRecordsPerFile": "1000",
        "overwriteSchema": "true",
    }
    assert writer.save_as_table_name == "dbo.customers_raw"
    assert writer.saved_path is None


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


def test_write_delta_keeps_raw_behavior_for_https_uri(dummy_spark):
    df = DummyDataFrame()
    lakehouse.write_delta(
        None,
        df,
        "https://onelake.dfs.fabric.microsoft.com/workspaces/ws/TablesLakehouse/Files/raw/snapshot",
        {"mode": "append"},
    )
    writer = df.write
    assert writer.save_as_table_name is None
    assert writer.saved_path == "https://onelake.dfs.fabric.microsoft.com/workspaces/ws/TablesLakehouse/Files/raw/snapshot"
    assert writer.mode_calls[-1] == "append"


def test_write_delta_resolves_lakehouse_files_uri(dummy_spark, monkeypatch):
    df = DummyDataFrame()
    monkeypatch.setattr(
        lakehouse,
        "resolve_fabric_files_path",
        lambda ref: f"/lakehouse/{ref.lakehouse}/Files/{ref.subpath}",
    )
    lakehouse.write_delta(
        None,
        df,
        "lakehouse://mvp-lakehouse/files/raw/customers/data.parquet",
        {"mode": "overwrite"},
    )
    writer = df.write
    assert writer.save_as_table_name is None
    assert (
        writer.saved_path
        == "/lakehouse/mvp-lakehouse/Files/raw/customers/data.parquet"
    )
    assert writer.mode_calls[-1] == "overwrite"
