from datacore.io import readers
from datacore.platforms.base import LocalPlatform


def test_read_batch_storage_infer_schema(tmp_path, spark):
    path = tmp_path / "data.csv"
    df = spark.createDataFrame([(1, "A")], ["id", "name"])
    df.write.mode("overwrite").option("header", True).csv(str(path))
    platform = LocalPlatform({"checkpoint_base": str(tmp_path / "chk")})

    result = readers.read_batch(
        spark,
        platform,
        {
            "type": "storage",
            "format": "csv",
            "uri": str(path),
            "backend": "local",
            "infer_schema": True,
            "options": {"header": "true"},
        },
        layer="bronze",
        dataset="customers",
        environment="dev",
    )

    assert result.schema[0].name == "id"
    cache_path = (tmp_path / "chk" / "bronze" / "customers" / "_schema_cache" / "schema.json")
    assert cache_path.exists()


def test_read_batch_api_rest(monkeypatch, spark, tmp_path):
    platform = LocalPlatform({"checkpoint_base": str(tmp_path / "chk")})

    monkeypatch.setattr(
        readers.rest,
        "fetch_pages",
        lambda cfg: [{"items": [{"id": 1, "info": {"name": "Ada"}}]}],
    )

    result = readers.read_batch(
        spark,
        platform,
        {
            "type": "api_rest",
            "record_path": "items",
            "flatten": True,
        },
        layer="bronze",
        dataset="api_customers",
        environment="dev",
    )

    assert result.collect()[0]["info.name"] == "Ada"


def test_read_batch_api_graphql(monkeypatch, spark, tmp_path):
    platform = LocalPlatform({"checkpoint_base": str(tmp_path / "chk")})

    monkeypatch.setattr(
        readers.graphql,
        "execute_query",
        lambda cfg: {"data": {"customers": [{"id": 1, "email": "ada@example.com"}]}},
    )

    result = readers.read_batch(
        spark,
        platform,
        {
            "type": "api_graphql",
            "url": "https://example/graphql",
            "query": "query { customers { id email } }",
            "record_path": ["data", "customers"],
        },
        layer="silver",
        dataset="graphql_customers",
        environment="dev",
    )

    assert result.count() == 1
    assert result.collect()[0]["email"] == "ada@example.com"
