from datacore.connectors.db import jdbc


class _FakeReader:
    def __init__(self):
        self.options: dict[str, object] = {}

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        return self.options


class _FakeSpark:
    def __init__(self):
        self._reader = _FakeReader()

    @property
    def read(self):  # pragma: no cover - accessor pattern
        return self

    def format(self, fmt):
        assert fmt == "jdbc"
        return self._reader


def test_jdbc_read_injects_partitioning_and_pushdown():
    spark = _FakeSpark()
    result = jdbc.read(
        spark,
        {
            "url": "jdbc:postgresql://host/db",
            "table": "public.orders",
            "pushdown": True,
            "partition_column": "id",
            "lower_bound": 1,
            "upper_bound": 100,
            "num_partitions": 4,
            "fetchsize": 1_000,
        },
    )

    assert result["url"] == "jdbc:postgresql://host/db"
    assert result["dbtable"] == "public.orders"
    assert result["partitionColumn"] == "id"
    assert result["lowerBound"] == 1
    assert result["upperBound"] == 100
    assert result["numPartitions"] == 4
    assert result["fetchsize"] == 1_000
    assert result["pushDownPredicate"] == "true"


def test_jdbc_read_supports_partitioning_block_aliases():
    spark = _FakeSpark()
    result = jdbc.read(
        spark,
        {
            "url": "jdbc:postgresql://host/db",
            "table": "public.orders",
            "partitioning": {
                "partitionColumn": "id",
                "lower_bound": 10,
                "upperBound": 20,
                "num_partitions": 2,
                "fetchsize": 5_000,
            },
            "predicate_pushdown": False,
        },
    )

    assert result["partitionColumn"] == "id"
    assert result["lowerBound"] == 10
    assert result["upperBound"] == 20
    assert result["numPartitions"] == 2
    assert result["fetchsize"] == 5_000
    assert result["pushDownPredicate"] == "false"


def test_jdbc_write_honours_snake_case_options():
    captured = {}

    class _Writer:
        def __init__(self):
            self.opts = {}

        def option(self, key, value):
            self.opts[key] = value
            return self

        def mode(self, mode):
            captured["mode"] = mode
            return self

        def format(self, fmt):
            assert fmt == "jdbc"
            return self

        def save(self):
            captured["options"] = dict(self.opts)

    class _FakeDf:
        def __init__(self):
            self.write = _Writer()

    jdbc.write(
        _FakeDf(),
        {
            "url": "jdbc:postgresql://host/db",
            "table": "public.orders",
            "batch_size": 500,
            "isolation_level": "READ_COMMITTED",
            "create_table_options": "WITH (FILLFACTOR=80)",
            "truncate_safe": True,
        },
    )

    assert captured["mode"] == "overwrite"
    opts = captured["options"]
    assert opts["batchsize"] == 500
    assert opts["isolationLevel"] == "READ_COMMITTED"
    assert opts["createTableOptions"].startswith("WITH")
    assert opts["truncate"] == "true"
