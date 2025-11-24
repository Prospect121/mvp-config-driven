import importlib

import pytest
from pyspark.sql import Row


@pytest.mark.parametrize(
    "module_name",
    [
        "datacore.connectors.storage.abfs",
        "datacore.connectors.storage.gcs",
        "datacore.connectors.storage.s3",
    ],
)
def test_storage_connector_write(monkeypatch, spark, module_name):
    module = importlib.import_module(module_name)
    df = spark.createDataFrame([Row(id=1, partition="a")])

    captured: dict[str, object] = {"options": {}}

    from pyspark.sql.readwriter import DataFrameWriter

    def fake_mode(self, value):  # type: ignore[override]
        captured["mode"] = value
        return self

    def fake_format(self, fmt):  # type: ignore[override]
        captured["format"] = fmt
        return self

    def fake_partitionBy(self, *cols):  # type: ignore[override]
        captured["partition_by"] = cols
        return self

    def fake_option(self, key, value):  # type: ignore[override]
        captured["options"][key] = value
        return self

    def fake_save(self, uri):  # type: ignore[override]
        captured["uri"] = uri
        return None

    monkeypatch.setattr(DataFrameWriter, "mode", fake_mode, raising=False)
    monkeypatch.setattr(DataFrameWriter, "format", fake_format, raising=False)
    monkeypatch.setattr(DataFrameWriter, "partitionBy", fake_partitionBy, raising=False)
    monkeypatch.setattr(DataFrameWriter, "option", fake_option, raising=False)
    monkeypatch.setattr(DataFrameWriter, "save", fake_save, raising=False)

    module.write(
        df,
        "file:///tmp/out",
        "parquet",
        "append",
        {"compression": "snappy"},
        partition_by=["partition"],
        merge_schema=True,
    )

    assert captured["mode"] == "append"
    assert captured["format"] == "parquet"
    assert captured["partition_by"] == ("partition",)
    assert captured["options"]["compression"] == "snappy"
    assert captured["options"]["mergeSchema"] == "true"
    assert captured["uri"] == "file:///tmp/out"


@pytest.mark.parametrize(
    "module_name",
    [
        "datacore.connectors.storage.abfs",
        "datacore.connectors.storage.gcs",
        "datacore.connectors.storage.s3",
    ],
)
def test_storage_connector_read(tmp_path, spark, module_name):
    module = importlib.import_module(module_name)
    source_path = tmp_path / "data.parquet"
    spark.createDataFrame([Row(id=1)]).write.mode("overwrite").parquet(str(source_path))

    df = module.read(
        spark,
        str(source_path),
        "parquet",
        {"mergeSchema": "true"},
        schema=None,
    )

    assert df.count() == 1
