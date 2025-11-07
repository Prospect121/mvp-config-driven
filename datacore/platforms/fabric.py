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
        active = SparkSession.getActiveSession()
        if active is not None:
            LOGGER.info(
                "Reusing active SparkSession for Fabric",
                extra={"platform": self.name, "event": "spark_session_reuse_active"},
            )
            return active

        default = SparkSession.getDefaultSession()
        if default is not None:
            LOGGER.info(
                "Reusing default SparkSession for Fabric",
                extra={"platform": self.name, "event": "spark_session_reuse_default"},
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
            try:
                session = SparkSession(active_sc)
                LOGGER.info(
                    "Attached to existing active SparkContext for Fabric",
                    extra={
                        "platform": self.name,
                        "event": "spark_session_attach_active_sparkcontext",
                    },
                )
                return session
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.error(
                    "Unable to attach to existing SparkContext in Fabric",
                    extra={
                        "platform": self.name,
                        "event": "spark_session_attach_failed",
                        "error": str(exc),
                    },
                )
                raise RuntimeError(
                    "FabricPlatform detected an existing SparkContext but could not "
                    "attach a SparkSession. Do not create a new SparkContext in Fabric; "
                    "ensure you are running inside a managed Fabric Spark environment."
                ) from exc

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
