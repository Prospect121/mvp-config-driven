from datacore.io import readers
from datacore.platforms.base import LocalPlatform


def test_read_batch_local(tmp_path, spark):
    path = tmp_path / "data.parquet"
    df = spark.createDataFrame([(1, "A")], ["id", "name"])
    df.write.parquet(str(path))
    platform = LocalPlatform({"checkpoint_base": str(tmp_path / "chk")})
    result = readers.read_batch(
        spark,
        platform,
        {"type": "storage", "format": "parquet", "uri": str(path), "backend": "local"},
    )
    assert result.count() == 1
