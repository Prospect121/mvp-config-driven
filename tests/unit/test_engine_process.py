from pyspark.sql import Row

from datacore.core import engine, validation
from datacore.core.validation import ValidationResult
from datacore.platforms.base import LocalPlatform


def test_process_dataset_batch_flow(monkeypatch, spark, tmp_path):
    platform = LocalPlatform({"checkpoint_base": str(tmp_path / "chk")})
    dataset = {
        "name": "customers",
        "layer": "silver",
        "source": {"type": "storage", "uri": "file:///tmp/in", "backend": "local"},
        "sink": {"type": "storage", "uri": str(tmp_path / "out"), "backend": "local"},
        "transform": {},
        "validation": {
            "quarantine_sink": {
                "type": "storage",
                "uri": str(tmp_path / "quarantine"),
                "backend": "local",
            }
        },
    }

    source_df = spark.createDataFrame([Row(id=1)])
    valid_df = spark.createDataFrame([Row(id=1, _reject_reason="")])
    invalid_df = spark.createDataFrame([Row(id=2, _reject_reason="bad")])
    quarantine_df = invalid_df
    metrics = {
        "input_rows": 2,
        "valid_rows": 1,
        "invalid_rows": 1,
        "by_rule": {"expect_not_null:id": {"invalid_rows": 1, "valid_rows": 1}},
    }
    result_obj = ValidationResult(
        valid_df=valid_df,
        invalid_df=invalid_df,
        quarantine_df=quarantine_df,
        metrics=metrics,
    )

    monkeypatch.setattr(engine.readers, "read_batch", lambda *args, **kwargs: source_df)
    monkeypatch.setattr(validation, "apply_validation", lambda df, cfg: result_obj)
    monkeypatch.setattr(engine, "prepare_incremental", lambda df, sink, cfg, platform: (df, sink, False))

    batch_calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(engine.writers, "write_batch", lambda df, plat, sink: batch_calls.append(("batch", sink)))
    rejects: list[dict] = []
    monkeypatch.setattr(engine.writers, "write_rejects", lambda df, plat, sink, **kw: rejects.append(sink))
    metrics_calls: list[dict] = []
    monkeypatch.setattr(
        engine.writers,
        "write_metrics",
        lambda spark, plat, sink, **kw: metrics_calls.append({"sink": sink, **kw}),
    )
    monkeypatch.setattr(engine.observability, "record_dataset", lambda *a, **k: None)

    result = engine._process_dataset(
        layer="silver",
        dataset=dataset,
        platform=platform,
        environment="dev",
        spark=spark,
        run_id="run-1",
        dry_run=False,
    )

    assert result["status"] == "completed"
    assert len(batch_calls) == 2
    assert rejects and rejects[0]["type"] == "storage"
    assert metrics_calls and metrics_calls[0]["run_id"] == "run-1"
    assert metrics_calls[0]["metrics"]["quarantine_rows"] == 1


def test_process_dataset_streaming_flow(monkeypatch, spark, tmp_path):
    platform = LocalPlatform({"checkpoint_base": str(tmp_path / "chk")})
    dataset = {
        "name": "orders_stream",
        "layer": "bronze",
        "streaming": {
            "enabled": True,
            "trigger": "5 minutes",
            "checkpoint_location": str(tmp_path / "chk"),
        },
        "source": {"type": "kafka", "options": {"subscribe": "orders"}},
        "sink": {"type": "nosql", "engine": "cosmosdb", "options": {}},
    }

    dummy_df = object()
    monkeypatch.setattr(engine.readers, "read_stream", lambda *a, **k: dummy_df)
    monkeypatch.setattr(engine, "_apply_streaming_options", lambda df, cfg: df)
    monkeypatch.setattr(engine, "_apply_transformations", lambda df, cfg: df)

    writes: list[dict[str, object]] = []

    def fake_write_stream(df, platform, sink, checkpoint, trigger=None):  # type: ignore[no-untyped-def]
        writes.append({
            "df": df,
            "sink": sink,
            "checkpoint": checkpoint,
            "trigger": trigger,
        })

    monkeypatch.setattr(engine.writers, "write_stream", fake_write_stream)
    monkeypatch.setattr(engine.observability, "record_dataset", lambda *a, **k: None)

    result = engine._process_dataset(
        layer="bronze",
        dataset=dataset,
        platform=platform,
        environment="dev",
        spark=spark,
        run_id="run-2",
        dry_run=False,
    )

    assert result["status"] == "streaming"
    assert writes and writes[0]["df"] is dummy_df
    assert writes[0]["checkpoint"] == str(tmp_path / "chk")
    assert writes[0]["trigger"] == "5 minutes"
