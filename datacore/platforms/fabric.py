"""Plataforma para Microsoft Fabric."""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from datacore.platforms.base import PlatformBase
from datacore.utils.logging import get_logger


LOGGER = get_logger(__name__)


class FabricPlatform(PlatformBase):
    name = "fabric"

    def build_spark_session(self, opts: dict[str, Any] | None = None) -> SparkSession:
        active = SparkSession.getActiveSession()
        if active is not None:
            LOGGER.info(
                "Reusing active SparkSession",
                extra={"platform": self.name, "event": "spark_session_reuse"},
            )
            return active

        builder = SparkSession.builder.appName(
            self.config.get("app_name", "datacore-fabric")
        )
        for key, value in (opts or {}).items():
            builder = builder.config(key, value)
        for key, value in self.config.get("extra_conf", {}).items():
            builder = builder.config(key, value)

        session = builder.getOrCreate()
        LOGGER.info(
            "Creating new SparkSession",
            extra={"platform": self.name, "event": "spark_session_create"},
        )
        return session

    def _resolve_platform_secret(self, name: str) -> str:
        secrets = self.config.get("secrets", {})
        if name in secrets:
            return secrets[name]
        return ""
