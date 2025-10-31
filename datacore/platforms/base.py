"""Interfaces de plataformas."""

from __future__ import annotations

import abc
import os
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

    def resolve_secret(self, name: str) -> str:
        """Obtiene un secreto desde la plataforma o variables de entorno."""

        if name.startswith("SECRET:"):
            resolved = self._resolve_platform_secret(name.split(":", 1)[1])
            if resolved:
                return resolved
        env_value = os.getenv(name)
        if env_value:
            return env_value
        config_value = self.config.get("secrets", {}).get(name)
        if config_value:
            return config_value
        return ""

    @abc.abstractmethod
    def _resolve_platform_secret(self, name: str) -> str:
        """Implementado por cada plataforma para resolver secretos nativos."""

    def resolve_secret_reference(self, value: str) -> str:
        """Resuelve expresiones del tipo ${ENV} o ${SECRET:NAME}."""

        if value.startswith("${") and value.endswith("}"):
            inner = value[2:-1]
            return self.resolve_secret(inner)
        if value.startswith("SECRET:"):
            return self.resolve_secret(value)
        env_value = os.getenv(value)
        if env_value:
            return env_value
        return value

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

    def _resolve_platform_secret(self, name: str) -> str:
        return self.config.get("secrets", {}).get(name, "")
