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
    class DummySpark:
        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return active_session

    monkeypatch.setattr("datacore.platforms.fabric.SparkSession", DummySpark)

    platform = FabricPlatform(config={})

    assert platform.build_spark_session({}) is active_session


def test_prepare_spark_reuses_global_active_session(monkeypatch, active_session):
    class DummySpark:
        @staticmethod
        def getActiveSession():  # noqa: N802 - keep pyspark naming
            return active_session

    monkeypatch.setattr("datacore.core.engine.SparkSession", DummySpark)

    platform = FabricPlatform(config={})

    assert _prepare_spark(platform, {"spark": {"extra_conf": {"foo": "bar"}}}) is active_session
