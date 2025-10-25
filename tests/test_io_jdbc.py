import pandas as pd
import pytest

from datacore.io.jdbc import JDBCConfigurationError, read_jdbc


class DummyDataFrame:
    def __init__(self, rows):
        self._rows = rows

    def toPandas(self):
        return pd.DataFrame(self._rows)

    def collect(self):  # pragma: no cover - used only if pandas unavailable
        return self._rows


class DummyReader:
    def __init__(self):
        self._options = {"format": None}

    def format(self, fmt):
        self._options["format"] = fmt
        return self

    def option(self, key, value):
        self._options[key] = value
        return self

    def load(self):
        query = self._options.get("query", "")
        if "MIN(" in str(query):
            return DummyDataFrame([
                {"lower_bound": 1, "upper_bound": 99},
            ])
        return DummyDataFrame([
            {"id": 1, "updated_at": "2024-01-01T00:00:00Z"},
            {"id": 2, "updated_at": "2024-01-02T00:00:00Z"},
        ])


class DummySparkSession:
    def __init__(self):
        self.reader = DummyReader()

    @property
    def read(self):
        return self.reader


def test_read_jdbc_injects_tls_defaults():
    spark = DummySparkSession()
    cfg = {
        "url": "jdbc:postgresql://example:5432/db",
        "driver": "org.postgresql.Driver",
        "query": "SELECT * FROM table WHERE updated_at >= '${RAW_WM_TS}'",
        "incremental": {"watermark": {"field": "updated_at", "value": "2024-01-01"}},
        "partitioning": {
            "column": "id",
            "lower_bound": 1,
            "upper_bound": 10,
            "num_partitions": 2,
        },
    }

    df, watermark = read_jdbc(cfg, spark)
    assert isinstance(df, DummyDataFrame)
    assert watermark == "2024-01-02T00:00:00Z"

    assert spark.reader._options["format"] == "jdbc"
    assert spark.reader._options["ssl"] == "true"
    assert spark.reader._options["partitionColumn"] == "id"
    assert spark.reader._options["lowerBound"] == 1
    assert spark.reader._options["upperBound"] == 10
    assert spark.reader._options["numPartitions"] == 2
    assert spark.reader._options["query"].startswith("SELECT * FROM table")


def test_read_jdbc_rejects_tls_disable():
    spark = DummySparkSession()
    cfg = {
        "url": "jdbc:mysql://example:3306/db",
        "driver": "com.mysql.cj.jdbc.Driver",
        "query": "SELECT 1",
        "options": {"useSSL": "false"},
    }

    with pytest.raises(JDBCConfigurationError):
        read_jdbc(cfg, spark)


def test_read_jdbc_auto_discovers_bounds():
    spark = DummySparkSession()
    cfg = {
        "url": "jdbc:postgresql://example:5432/db",
        "driver": "org.postgresql.Driver",
        "table": "transactions",
        "partitioning": {"column": "id", "num_partitions": 4},
    }

    df, watermark = read_jdbc(cfg, spark)
    assert isinstance(df, DummyDataFrame)
    assert watermark is None
    assert spark.reader._options["partitionColumn"] == "id"
    assert spark.reader._options["lowerBound"] == 1
    assert spark.reader._options["upperBound"] == 99
    assert spark.reader._options["numPartitions"] == 4


def test_read_jdbc_managed_identity_auth():
    spark = DummySparkSession()
    cfg = {
        "url": "jdbc:sqlserver://example:1433/db",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "table": "transactions",
        "auth": {"mode": "managed_identity"},
    }

    read_jdbc(cfg, spark)

    assert spark.reader._options["azure.identity.auth.type"] == "ManagedIdentity"
