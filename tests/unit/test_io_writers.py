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
        metrics={"input_rows": 1},
    )
    metrics_dir = tmp_path / "target" / "_metrics"
    assert any(metrics_dir.iterdir())
