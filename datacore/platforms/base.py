"""Interfaces de plataformas."""

from __future__ import annotations

import abc
from typing import Any

from pyspark.sql import SparkSession

from datacore.utils.paths import normalize_uri


class PlatformBase(abc.ABC):
    """Interfaz común para plataformas cloud."""

    name: str

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {}

    @abc.abstractmethod
    def build_spark_session(self, opts: dict[str, Any] | None = None) -> SparkSession:
        """Crea la SparkSession con configuraciones específicas."""

    @abc.abstractmethod
    def resolve_secret(self, name: str) -> str:
        """Obtiene un secreto desde la plataforma."""

    def normalize_uri(self, uri: str) -> str:
        return normalize_uri(uri)

    def checkpoint_dir(self, layer: str, dataset: str, env: str) -> str:
        base = self.config.get("checkpoint_base", f"/tmp/datacore/{env}")
        return f"{base}/{layer}/{dataset}"


class LocalPlatform(PlatformBase):
    name = "local"

    def build_spark_session(self, opts: dict[str, Any] | None = None) -> SparkSession:
        builder = SparkSession.builder.appName(self.config.get("app_name", "datacore-local"))
        for key, value in (opts or {}).items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    def resolve_secret(self, name: str) -> str:
        return self.config.get("secrets", {}).get(name, "")
