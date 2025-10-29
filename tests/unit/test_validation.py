from datacore.core.validation import apply_validation


def test_apply_validation_not_null(spark):
    df = spark.createDataFrame([(1, "a"), (2, None)], ["id", "value"])
    metrics = apply_validation(df, {"expect_not_null": ["value"]})
    assert metrics["invalid_rows"] == 1
    assert metrics["valid_rows"] == 1
