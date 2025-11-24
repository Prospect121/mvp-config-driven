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
        opts = opts or {}
        extra_conf = self.config.get("extra_conf", {})
        config_items = list(opts.items()) + list(extra_conf.items())

        def _log(event: str, message: str) -> None:
            LOGGER.info(message, extra={"platform": self.name, "event": event})

        get_active = getattr(SparkSession, "getActiveSession", None)
        if callable(get_active):
            active_session = get_active()
            if active_session is not None:
                _log("spark_session_reuse", "Reusing active SparkSession for Fabric")
                return active_session

        get_default = getattr(SparkSession, "getDefaultSession", None)
        if callable(get_default):
            default_session = get_default()
            if default_session is not None:
                _log("spark_session_reuse", "Reusing default SparkSession for Fabric")
                return default_session

        instantiated_session = getattr(SparkSession, "_instantiatedSession", None)
        if instantiated_session is not None:
            _log("spark_session_reuse", "Reusing instantiated SparkSession for Fabric")
            return instantiated_session

        get_active_context = getattr(SparkContext, "getActive", None)
        active_context = None
        if callable(get_active_context):
            active_context = get_active_context()
        if active_context is None:
            active_context = getattr(SparkContext, "_active_spark_context", None)

        if active_context is not None:
            session = SparkSession(active_context)
            for key, value in config_items:
                try:
                    session.conf.set(key, value)
                except Exception:  # pragma: no cover - depends on runtime support
                    LOGGER.debug(
                        "Could not apply Spark config when attaching to active context",
                        extra={
                            "platform": self.name,
                            "event": "spark_session_attach_config_skip",
                            "key": key,
                        },
                    )
            _log(
                "spark_session_attach_active_sparkcontext",
                "Attached to existing active SparkContext for Fabric",
            )
            return session

        builder = SparkSession.builder.appName(
            self.config.get("app_name", "datacore-fabric")
        )

        for key, value in config_items:
            builder = builder.config(key, value)

        session = builder.getOrCreate()
        _log("spark_session_create", "Creating new SparkSession for Fabric (no active context found)")
        return session

    def _resolve_platform_secret(self, name: str) -> str:
        secrets = self.config.get("secrets", {})
        if name in secrets:
            return secrets[name]
        return ""
