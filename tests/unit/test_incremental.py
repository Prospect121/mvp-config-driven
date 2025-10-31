from datacore.core.incremental import handle_incremental


def test_handle_incremental_merge_storage(tmp_path, spark):
    path = tmp_path / "dataset"
    existing = spark.createDataFrame(
        [(1, "old", "2024-01-01 00:00:00"), (2, "keep", "2024-01-02 00:00:00")],
        ["id", "value", "_ingestion_ts"],
    )
    existing.write.mode("overwrite").parquet(str(path))

    new_df = spark.createDataFrame(
        [(1, "new", "2024-02-01 00:00:00"), (3, "fresh", "2024-02-02 00:00:00")],
        ["id", "value", "_ingestion_ts"],
    )

    handled = handle_incremental(
        new_df,
        {"type": "storage", "uri": str(path), "format": "parquet"},
        {"mode": "merge", "keys": ["id"], "order_by": ["_ingestion_ts DESC"]},
    )

    assert handled is True
    result = spark.read.parquet(str(path)).orderBy("id").collect()
    assert [row.value for row in result] == ["new", "keep", "fresh"]


def test_handle_incremental_non_merge_returns_false(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "value"])
    handled = handle_incremental(df, {"type": "storage", "uri": "/tmp/x"}, {"mode": "append"})
    assert handled is False
