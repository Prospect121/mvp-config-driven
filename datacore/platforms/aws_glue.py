"""Plataforma AWS Glue."""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from datacore.platforms.base import PlatformBase


class AwsGluePlatform(PlatformBase):
    name = "aws"

    def build_spark_session(self, opts: dict[str, Any] | None = None) -> SparkSession:
        builder = SparkSession.builder.appName(self.config.get("app_name", "datacore-aws"))
        builder = builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        builder = builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        for key, value in (opts or {}).items():
            builder = builder.config(key, value)
        for key, value in self.config.get("extra_conf", {}).items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    def resolve_secret(self, name: str) -> str:
        secrets = self.config.get("secrets", {})
        if name in secrets:
            return secrets[name]
        raise KeyError(f"Secreto {name} no disponible en AWS config")
