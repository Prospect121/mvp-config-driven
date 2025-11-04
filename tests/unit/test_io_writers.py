from typing import Any
from unittest import mock

from pyspark.sql import Row

from datacore.io import writers
from datacore.platforms.base import LocalPlatform


def test_write_batch_storage_partition(tmp_path, spark):
    df = spark.createDataFrame([Row(id=1, country="mx"), Row(id=2, country="mx")])
    platform = LocalPlatform({})
    sink = {
        "type": "storage",
        "uri": str(tmp_path / "out"),
        "format": "parquet",
        "backend": "local",
        "partition_by": ["country"],
    }
    writers.write_batch(df, platform, sink)
    stored = spark.read.parquet(str(tmp_path / "out"))
    assert stored.count() == 2


def test_write_nosql_dynamodb(monkeypatch, spark):
    df = spark.createDataFrame([Row(id=1, value="A"), Row(id=2, value="B")])

    mock_batch = mock.MagicMock()
    mock_table = mock.MagicMock()
    mock_table.batch_writer.return_value.__enter__.return_value = mock_batch
    mock_resource = mock.MagicMock()
    mock_resource.Table.return_value = mock_table
    monkeypatch.setattr(writers, "boto3", mock.MagicMock(resource=lambda *args, **kwargs: mock_resource))

    sink = {
        "type": "nosql",
        "engine": "dynamodb",
        "table": "tbl",
        "batch_size": 1,
        "options": {"region": "us-east-1"},
    }
    writers.write_batch(df, LocalPlatform({}), sink)

    assert mock_batch.put_item.call_count == 2


def test_write_rejects_and_metrics(tmp_path, spark):
    df = spark.createDataFrame([Row(id=1, _reject_reason="bad")])
    platform = LocalPlatform({"checkpoint_base": str(tmp_path / "chk")})
    sink = {"type": "storage", "uri": str(tmp_path / "target"), "backend": "local"}

    writers.write_rejects(
        df,
        platform,
        sink,
        layer="silver",
        dataset="customers",
        environment="dev",
    )
    assert (tmp_path / "target" / "_rejects").exists()

    writers.write_metrics(
        spark,
        platform,
        sink,
        layer="silver",
        dataset="customers",
        environment="dev",
        run_id="run-1",
        metrics={"input_rows": 1},
    )
    metrics_dir = tmp_path / "target" / "_metrics"
    assert (metrics_dir / "run-1.json").exists()


def test_write_batch_bigquery(monkeypatch, spark):
    df = spark.createDataFrame([Row(id=1)])

    captured: dict[str, Any] = {"options": {}}

    from pyspark.sql.readwriter import DataFrameWriter

    def fake_format(self, fmt):  # type: ignore[override]
        captured["format"] = fmt
        return self

    def fake_mode(self, mode):  # type: ignore[override]
        captured["mode"] = mode
        return self

    def fake_option(self, key, value):  # type: ignore[override]
        captured["options"][key] = value
        return self

    def fake_save(self, path=None, format=None, mode=None, partitionBy=None, **kwargs):  # type: ignore[override]
        captured["path"] = path
        return None

    monkeypatch.setattr(DataFrameWriter, "format", fake_format, raising=False)
    monkeypatch.setattr(DataFrameWriter, "mode", fake_mode, raising=False)
    monkeypatch.setattr(DataFrameWriter, "option", fake_option, raising=False)
    monkeypatch.setattr(DataFrameWriter, "save", fake_save, raising=False)

    writers.write_batch(
        df,
        LocalPlatform({}),
        {
            "type": "warehouse",
            "engine": "bigquery",
            "table": "dataset.table",
            "mode": "overwrite",
            "temporary_gcs_bucket": "gs://tmp",
            "intermediate_format": "parquet",
            "options": {"writeMethod": "direct"},
        },
    )

    assert captured["path"] == "dataset.table"
    assert captured["format"] == "bigquery"
    assert captured["mode"] == "overwrite"
    assert captured["options"]["writeMethod"] == "direct"
    assert captured["options"]["temporaryGcsBucket"] == "gs://tmp"
    assert captured["options"]["intermediateFormat"] == "parquet"


def test_write_stream_storage(monkeypatch, spark):
    df = spark.createDataFrame([Row(id=1)])

    captured: dict[str, Any] = {}

    def fake_stream_writer(df, options, fmt, mode, checkpoint, trigger=None):  # type: ignore[no-untyped-def]
        captured["fmt"] = fmt
        captured["mode"] = mode
        captured["options"] = options
        captured["checkpoint"] = checkpoint
        captured["trigger"] = trigger

    monkeypatch.setattr(writers, "_stream_writer_common", fake_stream_writer)

    writers.write_stream(
        df,
        LocalPlatform({}),
        {
            "type": "storage",
            "format": "parquet",
            "uri": "file:///tmp/out",
            "mode": "append",
            "write_options": {"compression": "snappy"},
        },
        checkpoint="/tmp/chk",
        trigger="5 minutes",
    )

    assert captured["fmt"] == "parquet"
    assert captured["mode"] == "append"
    assert captured["options"]["path"] == "file:///tmp/out"
    assert captured["options"]["compression"] == "snappy"
    assert captured["checkpoint"] == "/tmp/chk"
    assert captured["trigger"] == "5 minutes"
