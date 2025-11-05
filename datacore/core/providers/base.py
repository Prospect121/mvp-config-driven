"""Interfaces base para providers opcionales."""

from __future__ import annotations

from typing import Any, Dict


class Provider:
    """Interfaz base para providers externos."""

    name: str = "base"

    def supports(self, feature: str) -> bool:
        """Indica si el provider implementa una capacidad concreta."""
        return False

    # ---- Shortcuts / Virtualización ----
    def ensure_shortcut(self, cfg: Dict[str, Any]) -> None:
        """Crea o actualiza un shortcut de forma idempotente."""
        raise NotImplementedError

    # ---- Lakehouse / Delta ----
    def write_table(self, df, table_uri: str, options: Dict[str, Any]) -> None:
        """Escribe un DataFrame en una tabla nativa del provider."""
        raise NotImplementedError

    def optimize(self, table_uri: str, options: Dict[str, Any]) -> None:
        """Ejecuta tareas de optimización sobre una tabla."""
        raise NotImplementedError

    # ---- Orchestration ----
    def ensure_spark_job(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Crea o actualiza una Spark Job Definition y devuelve metadatos."""
        raise NotImplementedError

    def ensure_pipeline(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Crea o actualiza una Data Pipeline y devuelve metadatos."""
        raise NotImplementedError
