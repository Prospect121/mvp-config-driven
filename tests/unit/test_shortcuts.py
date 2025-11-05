import pytest

from datacore.io import shortcuts
from datacore.platforms.base import LocalPlatform


@pytest.fixture(autouse=True)
def clear_registry_cache():
    shortcuts._load_registry.cache_clear()
    yield
    shortcuts._load_registry.cache_clear()


def test_resolve_storage_shortcut(spark, tmp_path):
    data = [(1, "foo"), (2, "bar")]
    df = spark.createDataFrame(data, ["id", "value"])
    target = tmp_path / "data.parquet"
    df.write.mode("overwrite").parquet(str(target))

    registry = tmp_path / "shortcuts.yml"
    registry.write_text(
        """
sales_s3:
  provider: storage
  backend: local
  target_uri: "{uri}"
  format: parquet
        """.strip().format(uri=str(target)),
        encoding="utf-8",
    )

    platform = LocalPlatform(config={"shortcuts_registry": str(registry)})

    result = shortcuts.resolve_shortcut(
        {"type": "shortcut", "name": "sales_s3"},
        platform,
        spark,
        layer="bronze",
        dataset="sales_shortcut",
        environment="dev",
    )

    assert sorted(result.collect()) == sorted(df.collect())


def test_shortcut_override_provider(spark, tmp_path):
    data = [("alice", 10)]
    table_path = tmp_path / "people.parquet"
    spark.createDataFrame(data, ["name", "score"]).write.mode("overwrite").parquet(str(table_path))

    registry = tmp_path / "shortcuts.yml"
    registry.write_text("{}", encoding="utf-8")
    platform = LocalPlatform(config={"shortcuts_registry": str(registry)})

    result = shortcuts.resolve_shortcut(
        {
            "type": "shortcut",
            "name": "inline",
            "provider": "storage",
            "backend": "local",
            "target_uri": str(table_path),
            "format": "parquet",
        },
        platform,
        spark,
        layer="silver",
        dataset="inline_shortcut",
        environment="dev",
    )

    assert result.count() == 1
