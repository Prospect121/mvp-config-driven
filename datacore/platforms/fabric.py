"""Plataforma para Microsoft Fabric."""

from __future__ import annotations

from typing import Any

from pyspark import SparkContext
from pyspark.sql import SparkSession

from datacore.platforms.base import PlatformBase
from datacore.utils.logging import get_logger


LOGGER = get_logger(__name__)


class FabricPlatform(PlatformBase):
    name = "fabric"
    reuse_active_session = True

    def build_spark_session(self, opts: dict[str, Any] | None = None) -> SparkSession:
        get_active = getattr(SparkSession, "getActiveSession", None)
        if callable(get_active):
            active = get_active()
            if active is not None:
                LOGGER.info(
                    "Reusing active SparkSession for Fabric",
                    extra={
                        "platform": self.name,
                        "event": "spark_session_reuse_active",
                    },
                )
                return active

        get_default = getattr(SparkSession, "getDefaultSession", None)
        if callable(get_default):
            default = get_default()
            if default is not None:
                LOGGER.info(
                    "Reusing default SparkSession for Fabric",
                    extra={
                        "platform": self.name,
                        "event": "spark_session_reuse_default",
                    },
                )
                return default

        instantiated = getattr(SparkSession, "_instantiatedSession", None)
        if instantiated is not None:
            LOGGER.info(
                "Reusing instantiated SparkSession for Fabric",
                extra={
                    "platform": self.name,
                    "event": "spark_session_reuse_instantiated",
                },
            )
            return instantiated

        active_sc = getattr(SparkContext, "_active_spark_context", None)
        if active_sc is not None:
            sc = SparkContext.getOrCreate()
            session = SparkSession(sc)
            LOGGER.info(
                "Attached to existing active SparkContext for Fabric",
                extra={
                    "platform": self.name,
                    "event": "spark_session_attach_active_sparkcontext",
                },
            )
            return session

        builder = SparkSession.builder.appName(
            self.config.get("app_name", "datacore-fabric")
        )

        for key, value in (opts or {}).items():
            builder = builder.config(key, value)

        for key, value in self.config.get("extra_conf", {}).items():
            builder = builder.config(key, value)

        session = builder.getOrCreate()
        LOGGER.info(
            "Creating new SparkSession for Fabric (no active context found)",
            extra={"platform": self.name, "event": "spark_session_create"},
        )
        return session

    def _resolve_platform_secret(self, name: str) -> str:
        secrets = self.config.get("secrets", {})
        if name in secrets:
            return secrets[name]
        return ""
