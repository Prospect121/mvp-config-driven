from pyspark.sql import functions as F

from datacore.core import federation


def test_apply_join_strategy(spark):
    left = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "letter"])
    right = spark.createDataFrame([(1, "uno"), (3, "tres")], ["id", "spanish"])
    dfs = {"api": left, "crm": right}
    config = {
        "strategy": "join",
        "join": {
            "left": "api",
            "right": "crm",
            "on": [{"left": "id", "right": "id"}],
            "join_type": "left",
            "select": [
                {"expr": "left.id", "as": "id"},
                {"expr": "left.letter", "as": "letter"},
                {"expr": "coalesce(right.spanish,'?')", "as": "spanish"},
            ],
        },
    }

    result = federation.apply_federation(dfs, config)

    rows = {row["id"]: row["spanish"] for row in result.collect()}
    assert rows == {1: "uno", 2: "?"}


def test_apply_union_strategy_allow_missing(spark):
    first = spark.createDataFrame([(1, "a")], ["id", "value"])
    second = spark.createDataFrame([(2,)], ["id"])
    dfs = {"f1": first, "f2": second}
    config = {
        "strategy": "union",
        "union": {"allow_missing_columns": True},
    }

    result = federation.apply_federation(dfs, config)
    assert result.orderBy(F.col("id")).collect()[1]["value"] is None
