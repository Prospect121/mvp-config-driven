from pyspark.sql import Row

from datacore.core import engine


def test_sanitize_options_masks_sensitive():
    options = {"Password": "secret", "path": "/tmp/data"}
    sanitized = engine._sanitize_options(options)
    assert sanitized["Password"] == "***"
    assert sanitized["path"] == "/tmp/data"


def test_merge_strategy_coalesce(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, value=None, _ingestion_ts="2024-01-01T00:00:00Z", __dc_source_ordinal=0),
            Row(id=1, value="B", _ingestion_ts="2024-01-02T00:00:00Z", __dc_source_ordinal=1),
        ]
    )

    result = engine._merge_strategy(df, {"keys": ["id"], "prefer": "coalesce"})
    row = result.collect()[0]
    assert row.id == 1
    assert row.value == "B"


def test_merge_strategy_left_prefers_first(spark):
    df = spark.createDataFrame(
        [
            Row(id=1, value="LEFT", _ingestion_ts="2024-01-01T00:00:00Z", __dc_source_ordinal=0),
            Row(id=1, value="RIGHT", _ingestion_ts="2024-02-01T00:00:00Z", __dc_source_ordinal=1),
        ]
    )

    result = engine._merge_strategy(
        df,
        {"keys": ["id"], "prefer": "left", "order_by": ["_ingestion_ts DESC"]},
    )
    row = result.collect()[0]
    assert row.value == "LEFT"


def test_detect_dataset_issues_multisource():
    dataset = {
        "name": "customers",
        "source": [{"type": "storage"}, {"type": "storage"}],
        "sink": {"type": "storage", "format": "txt"},
        "incremental": {"mode": "merge"},
    }
    issues = engine._detect_dataset_issues(dataset)
    assert any("merge_strategy" in issue for issue in issues)
    assert any("Formato" in issue for issue in issues)


def test_apply_transformations_adds_ingestion_ts(spark):
    df = spark.createDataFrame([Row(id=1, value=1)])
    transformed = engine._apply_transformations(
        df,
        {
            "sql": ["SELECT id, value FROM _src_0"],
            "ops": [{"rename": {"value": "amount"}}],
            "add_ingestion_ts": True,
        },
    )
    assert "_ingestion_ts" in transformed.columns
    assert "amount" in transformed.columns


def test_build_plan_masks_sink_options():
    dataset = {
        "name": "customers",
        "layer": "silver",
        "source": {"type": "storage", "uri": "file:///tmp", "options": {"password": "secret"}},
        "sink": {
            "type": "warehouse",
            "format": "delta",
            "options": {"connectionString": "abc"},
            "write_options": {"apiKey": "hidden"},
        },
        "transform": {"ops": [{"exclude": ["tmp"]}]},
        "incremental": {"mode": "append"},
        "streaming": {"enabled": False},
    }

    plan = engine._build_plan(dataset)

    source_options = plan["source_plan"]["sources"][0]["options"]
    assert source_options["password"] == "***"
    sink_options = plan["sink_plan"]["options"]
    assert sink_options["connectionString"] == "***"
    assert plan["transform_plan"]["ops"] == [{"exclude": ["tmp"]}]
