from datetime import datetime

from pyspark.sql import Row

from datacore.core import ops


def test_apply_ops_transformations(spark):
    df = spark.createDataFrame([(1, " ada ", "foo"), (2, "bea", "bar")], ["id", "name", "payload"])

    result = ops.apply_ops(
        df,
        [
            {"rename": {"name": "NAME"}},
            {"trim": ["NAME"]},
            {"uppercase": ["NAME"]},
            {"drop_columns": ["payload"]},
        ],
    )

    assert "payload" not in result.columns
    assert result.collect()[0]["NAME"] == "ADA"


def test_deduplicate_and_flatten(spark):
    df = spark.createDataFrame(
        [
            (1, Row(info=Row(value="A")), datetime(2024, 1, 1, 0, 0, 0)),
            (1, Row(info=Row(value="B")), datetime(2024, 2, 1, 0, 0, 0)),
        ],
        ["id", "data", "_ingestion_ts"],
    )

    result = ops.apply_ops(
        df,
        [
            {"flatten_json": {"prefix": "flat"}},
            {"deduplicate": {"keys": ["id"], "order_by": ["_ingestion_ts DESC"]}},
        ],
    )

    row = result.collect()[0]
    assert row.id == 1
    assert row.flatdata_info_value == "B"


def test_ops_respect_canonical_order(spark):
    df = spark.createDataFrame([(" 10.5 ", "tmp")], ["amount_raw", "dummy"])

    transformed = ops.apply_ops(
        df,
        [
            {"cast": {"amount": "double"}},
            {"rename": {"amount_raw": "amount"}},
            {"exclude": ["dummy"]},
        ],
    )

    row = transformed.collect()[0]
    assert "dummy" not in transformed.columns
    assert row.amount == 10.5


def test_ops_sql_stage_runs_last(spark):
    df = spark.createDataFrame([(1, "mx"), (2, "us")], ["id", "country"])

    result = ops.apply_ops(
        df,
        [
            {"sql": "SELECT id FROM __dc_ops WHERE country = 'mx'"},
        ],
    )

    rows = result.collect()
    assert rows[0]["id"] == 1
