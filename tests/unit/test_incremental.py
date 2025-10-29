from datacore.core.incremental import handle_incremental


def test_handle_incremental_append(tmp_path, spark):
    path = tmp_path / "dataset"
    df = spark.createDataFrame([(1, "A")], ["id", "value"])
    handle_incremental(df, {"uri": str(path), "format": "parquet"}, "append", [])
    assert spark.read.parquet(str(path)).count() == 1
