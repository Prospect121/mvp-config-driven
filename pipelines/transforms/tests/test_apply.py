import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.transforms.apply import apply_sql_transforms


def test_apply_sql_transforms_basic():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    try:
        df = spark.createDataFrame([
            (1, 10.0, "2025-09-01T10:00:00Z"),
            (2, None, "2025-09-01T11:00:00Z"),
            (3, 5.0, "2025-09-01T12:00:00Z"),
        ], ["id", "amount", "created_at"])

        transforms = {
            "sql": [
                # Selecciona registros no nulos y agrega columna de control
                {
                    "name": "filter_non_null",
                    "statement": "SELECT id, amount, created_at FROM __df__ WHERE amount IS NOT NULL",
                },
                {
                    "name": "add_flag",
                    "statement": "SELECT *, CASE WHEN amount > 9 THEN 1 ELSE 0 END AS flag FROM __df__",
                },
            ]
        }

        out = apply_sql_transforms(df, transforms)
        assert out.count() == 2
        cols = set(out.columns)
        assert "flag" in cols
        assert out.select(F.sum("flag")).collect()[0][0] == 1
    finally:
        spark.stop()