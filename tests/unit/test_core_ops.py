from datetime import datetime

from pyspark.sql import Row

from datacore.core import ops


def test_apply_ops_transformations(spark):
    df = spark.createDataFrame([(1, " ada ", "foo"), (2, "bea", "bar")], ["id", "name", "payload"])

    result = ops.apply_ops(
        df,
        [
            {"trim": ["name"]},
            {"uppercase": ["name"]},
            {"rename": {"name": "NAME"}},
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
