from datacore.core import cache, pushdown


def test_pushdown_generates_sql():
    source = {"type": "jdbc", "table": "public.orders"}
    ops = [{"filter": ["status = 'NEW'", "amount > 10"]}]
    sql = pushdown.try_pushdown(source, ops)
    assert sql == "SELECT * FROM public.orders WHERE (status = 'NEW') AND (amount > 10)"


def test_cache_materializes_dataset(spark, tmp_path):
    df = spark.createDataFrame([(1, "foo")], ["id", "value"])
    cfg = {"enabled": True, "storage": str(tmp_path), "ttl": "1h"}
    context = {"dataset": "demo", "signature": "abc123"}

    cached = cache.with_cache(df, cfg, context)
    assert cached.count() == 1

    second = cache.with_cache(df, cfg, context)
    assert second.count() == 1
