"""Plataforma Azure Databricks."""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from datacore.platforms.base import PlatformBase


class AzureDatabricksPlatform(PlatformBase):
    name = "azure"

    def build_spark_session(self, opts: dict[str, Any] | None = None) -> SparkSession:
        builder = SparkSession.builder.appName(self.config.get("app_name", "datacore-azure"))
        builder = builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        for key, value in (opts or {}).items():
            builder = builder.config(key, value)
        for key, value in self.config.get("extra_conf", {}).items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    def resolve_secret(self, name: str) -> str:
        secrets = self.config.get("secrets", {})
        if name in secrets:
            return secrets[name]
        raise KeyError(f"Secreto {name} no disponible en Azure config")
