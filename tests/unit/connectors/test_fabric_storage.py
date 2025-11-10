from __future__ import annotations

from datacore.connectors.storage import fabric


class DummyWriter:
    def __init__(self):
        self.format_value = None
        self.partition_by = None
        self.options: dict[str, object] = {}
        self.mode_calls: list[str] = []
        self.saved_path: str | None = None
        self.saved_table: str | None = None

    def format(self, fmt):  # type: ignore[override]
        self.format_value = fmt
        return self

    def partitionBy(self, *cols):  # type: ignore[override]
        self.partition_by = cols
        return self

    def option(self, key, value):  # type: ignore[override]
        self.options[key] = value
        return self

    def mode(self, value):  # type: ignore[override]
        self.mode_calls.append(value)
        return self

    def saveAsTable(self, table):  # type: ignore[override]
        self.saved_table = table
        return None

    def save(self, path):  # type: ignore[override]
        self.saved_path = path
        return None


class DummyDataFrame:
    def __init__(self):
        self.write = DummyWriter()


class DummyReader:
    def __init__(self):
        self.format_value = None
        self.options: dict[str, object] = {}
        self.schema_value = None
        self.loaded_path: str | None = None

    def format(self, fmt):  # type: ignore[override]
        self.format_value = fmt
        return self

    def option(self, key, value):  # type: ignore[override]
        self.options[key] = value
        return self

    def schema(self, schema):  # type: ignore[override]
        self.schema_value = schema
        return self

    def load(self, path):  # type: ignore[override]
        self.loaded_path = path
        return f"loaded:{path}"


class DummySpark:
    def __init__(self):
        self.table_calls: list[str] = []
        self.read = DummyReader()

    def table(self, name):
        self.table_calls.append(name)
        return f"table:{name}"


def test_write_managed_table_with_explicit_schema():
    df = DummyDataFrame()
    fabric.write(
        df,
        "lakehouse://demo/tables/dbo/customers_raw",
        "delta",
        "overwrite",
        {"mergeSchema": True},
    )
    writer = df.write
    assert writer.format_value == "delta"
    assert writer.saved_table == "dbo.customers_raw"
    assert writer.saved_path is None
    assert "overwriteSchema" in writer.options
    assert writer.mode_calls[-1] == "overwrite"


def test_write_managed_table_uses_default_schema(monkeypatch):
    monkeypatch.setenv("FABRIC_DEFAULT_SCHEMA", "finance")
    df = DummyDataFrame()
    fabric.write(
        df,
        "lakehouse://demo/tables/customers_curated",
        "delta",
        "append",
        {},
    )
    writer = df.write
    assert writer.saved_table == "finance.customers_curated"
    assert writer.mode_calls[-1] == "append"


def test_read_managed_table_uses_catalog():
    spark = DummySpark()
    result = fabric.read(
        spark,
        "lakehouse://demo/tables/dbo/customers_raw",
        "delta",
        {"dummy": "1"},
        schema=None,
    )
    assert result == "table:dbo.customers_raw"
    assert spark.table_calls == ["dbo.customers_raw"]
    assert spark.read.loaded_path is None


def test_read_files_resolves_mount():
    spark = DummySpark()
    df = fabric.read(
        spark,
        "lakehouse://demo/files/raw/customers/data.csv",
        "csv",
        {"header": "true"},
        schema=None,
    )
    assert df == "loaded:/lakehouse/demo/Files/raw/customers/data.csv"
    assert spark.read.format_value == "csv"


def test_write_files_resolves_mount():
    df = DummyDataFrame()
    fabric.write(
        df,
        "lakehouse://demo/files/silver/customers",
        "delta",
        "overwrite",
        {},
    )
    writer = df.write
    assert writer.saved_path == "/lakehouse/demo/Files/silver/customers"
    assert writer.saved_table is None
    assert writer.mode_calls[-1] == "overwrite"
