"""Tests for SparkSession management across platforms."""

from __future__ import annotations

import pytest

from datacore.core.engine import _prepare_spark
from datacore.platforms.azure_databricks import AzureDatabricksPlatform
from datacore.platforms.fabric import FabricPlatform


@pytest.fixture
def active_session():
    return object()


def test_azure_platform_reuses_active_session(monkeypatch, active_session):
    class DummySpark:
        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return active_session

    monkeypatch.setattr("datacore.platforms.azure_databricks.SparkSession", DummySpark)

    platform = AzureDatabricksPlatform(config={})

    assert platform.build_spark_session({}) is active_session


def test_fabric_platform_reuses_active_session(monkeypatch, active_session):
    class DummyBuilder:
        def appName(self, _):  # pragma: no cover - should not be called
            raise AssertionError("builder.appName should not be invoked")

    class DummySpark:
        builder = DummyBuilder()

        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return active_session

        @staticmethod
        def getDefaultSession():  # noqa: N802
            raise AssertionError("getDefaultSession should not be called")

    monkeypatch.setattr("datacore.platforms.fabric.SparkSession", DummySpark)
    monkeypatch.setattr("datacore.platforms.fabric.SparkContext", object())

    platform = FabricPlatform(config={})

    assert platform.build_spark_session({}) is active_session


def test_fabric_platform_reuses_default_session(monkeypatch):
    default_session = object()

    class DummyBuilder:
        def appName(self, _):  # pragma: no cover - should not be called
            raise AssertionError("builder.appName should not be invoked")

    class DummySpark:
        builder = DummyBuilder()
        _instantiatedSession = None

        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return None

        @staticmethod
        def getDefaultSession():  # noqa: N802 - keep pyspark naming
            return default_session

    monkeypatch.setattr("datacore.platforms.fabric.SparkSession", DummySpark)
    monkeypatch.setattr("datacore.platforms.fabric.SparkContext", object())

    platform = FabricPlatform(config={})

    assert platform.build_spark_session({}) is default_session


def test_fabric_platform_reuses_instantiated_session(monkeypatch):
    instantiated_session = object()

    class DummyBuilder:
        def appName(self, _):  # pragma: no cover - should not be called
            raise AssertionError("builder.appName should not be invoked")

    class DummySpark:
        builder = DummyBuilder()
        _instantiatedSession = instantiated_session

        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return None

        @staticmethod
        def getDefaultSession():  # noqa: N802 - keep pyspark naming
            return None

    monkeypatch.setattr("datacore.platforms.fabric.SparkSession", DummySpark)
    monkeypatch.setattr("datacore.platforms.fabric.SparkContext", object())

    platform = FabricPlatform(config={})

    assert platform.build_spark_session({}) is instantiated_session


def test_fabric_platform_attaches_active_spark_context(monkeypatch):
    attached_session = object()

    class RecordingBuilder:
        def __init__(self):
            self.get_or_create_calls = 0

        def getOrCreate(self):  # noqa: N802 - keep pyspark naming
            self.get_or_create_calls += 1
            return attached_session

        def appName(self, _):  # pragma: no cover - should not be called
            raise AssertionError("appName should not be used when attaching")

        def config(self, *_args, **_kwargs):  # pragma: no cover - defensive
            raise AssertionError("config should not be used when attaching")

    class DummySpark:
        builder = RecordingBuilder()
        _instantiatedSession = None

        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return None

        @staticmethod
        def getDefaultSession():  # noqa: N802 - keep pyspark naming
            return None

    class DummySparkContext:
        _active_spark_context = object()

    monkeypatch.setattr("datacore.platforms.fabric.SparkSession", DummySpark)
    monkeypatch.setattr("datacore.platforms.fabric.SparkContext", DummySparkContext)

    platform = FabricPlatform(config={})

    session = platform.build_spark_session({})

    assert session is attached_session
    assert DummySpark.builder.get_or_create_calls == 1


def test_fabric_platform_creates_new_session_when_no_context(monkeypatch):
    created = object()

    class RecordingBuilder:
        def __init__(self):
            self.app_name = None
            self.configs = []

        def appName(self, name):
            self.app_name = name
            return self

        def config(self, key, value):
            self.configs.append((key, value))
            return self

        def getOrCreate(self):  # noqa: N802 - keep pyspark naming
            return created

    class DummySpark:
        builder = RecordingBuilder()
        _instantiatedSession = None

        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return None

        @staticmethod
        def getDefaultSession():  # noqa: N802 - keep pyspark naming
            return None

    class DummySparkContext:
        _active_spark_context = None

    monkeypatch.setattr("datacore.platforms.fabric.SparkSession", DummySpark)
    monkeypatch.setattr("datacore.platforms.fabric.SparkContext", DummySparkContext)

    platform = FabricPlatform(
        config={
            "app_name": "custom-app",
            "extra_conf": {"spark.sql.shuffle.partitions": "4"},
        }
    )

    session = platform.build_spark_session({"spark.executor.instances": "2"})

    assert session is created
    builder = DummySpark.builder
    assert builder.app_name == "custom-app"
    assert ("spark.executor.instances", "2") in builder.configs
    assert ("spark.sql.shuffle.partitions", "4") in builder.configs


def test_fabric_platform_handles_missing_getactive(monkeypatch):
    default_session = object()

    class DummyBuilder:
        def appName(self, _):  # pragma: no cover - should not be called
            raise AssertionError("builder.appName should not be invoked")

    class DummySpark:
        builder = DummyBuilder()
        _instantiatedSession = None

        @staticmethod
        def getDefaultSession():  # noqa: N802 - keep pyspark naming
            return default_session

    monkeypatch.setattr("datacore.platforms.fabric.SparkSession", DummySpark)
    monkeypatch.setattr("datacore.platforms.fabric.SparkContext", object())

    platform = FabricPlatform(config={})

    assert platform.build_spark_session({}) is default_session


def test_fabric_platform_handles_missing_getdefault(monkeypatch):
    instantiated_session = object()

    class DummyBuilder:
        def appName(self, _):  # pragma: no cover - should not be called
            raise AssertionError("builder.appName should not be invoked")

    class DummySpark:
        builder = DummyBuilder()
        _instantiatedSession = instantiated_session

        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return None

    monkeypatch.setattr("datacore.platforms.fabric.SparkSession", DummySpark)
    monkeypatch.setattr("datacore.platforms.fabric.SparkContext", object())

    platform = FabricPlatform(config={})

    assert platform.build_spark_session({}) is instantiated_session


def test_prepare_spark_reuses_global_active_session(monkeypatch, active_session):
    class DummySpark:
        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return active_session

    monkeypatch.setattr("datacore.core.engine.SparkSession", DummySpark)

    platform = FabricPlatform(config={})

    assert _prepare_spark(platform, {"spark": {"extra_conf": {"foo": "bar"}}}) is active_session
