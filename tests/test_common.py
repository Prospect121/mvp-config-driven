import logging
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipelines.common import maybe_config_s3a


class DummyConf:
    def __init__(self):
        self.settings = {}

    def set(self, key: str, value: str) -> None:
        self.settings[key] = value


class DummySpark:
    def __init__(self):
        self.conf = DummyConf()


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    for key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ENDPOINT_URL",
        "S3A_DISABLE_SSL",
        "CUSTOM_ACCESS",
        "CUSTOM_SECRET",
    ]:
        monkeypatch.delenv(key, raising=False)


def test_maybe_config_s3a_leaves_ssl_enabled_by_default(monkeypatch, caplog):
    spark = DummySpark()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA123456789")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "super-secret-value")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://example.com")

    with caplog.at_level(logging.INFO):
        maybe_config_s3a(spark, "s3a://bucket/path", {})

    # Credentials should be configured but redacted in logs
    assert spark.conf.settings["spark.hadoop.fs.s3a.access.key"] == "AKIA123456789"
    assert spark.conf.settings["spark.hadoop.fs.s3a.secret.key"] == "super-secret-value"
    assert "AKIA123456789" not in caplog.text
    assert "super-secret-value" not in caplog.text
    assert "AKIA***" in caplog.text
    assert "spark.hadoop.fs.s3a.connection.ssl.enabled" not in spark.conf.settings


def test_maybe_config_s3a_rejects_ssl_disable_flag(monkeypatch):
    spark = DummySpark()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAZZZ")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    with pytest.raises(ValueError):
        maybe_config_s3a(spark, "s3a://bucket/path", {"s3a_disable_ssl": True})


def test_maybe_config_s3a_rejects_env_ssl_disable(monkeypatch):
    spark = DummySpark()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAENV")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("S3A_DISABLE_SSL", "true")

    with pytest.raises(ValueError):
        maybe_config_s3a(spark, "s3a://bucket/path", {})


def test_maybe_config_s3a_reads_credentials_from_named_env(monkeypatch):
    spark = DummySpark()
    monkeypatch.setenv("CUSTOM_ACCESS", "AKIA_ENV_NAME")
    monkeypatch.setenv("CUSTOM_SECRET", "env-secret")

    maybe_config_s3a(
        spark,
        "s3a://bucket/path",
        {"s3a_access_key_env": "CUSTOM_ACCESS", "s3a_secret_key_env": "CUSTOM_SECRET"},
    )

    assert spark.conf.settings["spark.hadoop.fs.s3a.access.key"] == "AKIA_ENV_NAME"
    assert spark.conf.settings["spark.hadoop.fs.s3a.secret.key"] == "env-secret"
