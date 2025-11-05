"""Implementación del provider opcional para Microsoft Fabric."""

from __future__ import annotations

from typing import Any, Dict

from datacore.core.providers.base import Provider


class FabricProvider(Provider):
    """Fachada de capacidades Microsoft Fabric."""

    name = "fabric"

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or {}
        from .env import FabricEnv

        self.env = FabricEnv(self.cfg)
        self._capabilities = {"shortcuts", "lakehouse", "orchestration"}
        if getattr(self.env, "warehouse_id", None):
            self._capabilities.add("warehouse")
        if getattr(self.env, "eventhouse_id", None):
            self._capabilities.add("kql")

    def supports(self, feature: str) -> bool:
        return feature in self._capabilities

    # ---- Shortcuts ----
    def ensure_shortcut(self, cfg: Dict[str, Any]) -> None:
        from .shortcuts import ensure_shortcut

        ensure_shortcut(self.env, cfg)

    # ---- Lakehouse ----
    def write_table(self, df, table_uri: str, options: Dict[str, Any]) -> None:
        from .lakehouse import write_delta

        write_delta(self.env, df, table_uri, options)

    def optimize(self, table_uri: str, options: Dict[str, Any]) -> None:
        from .lakehouse import optimize_delta

        optimize_delta(self.env, table_uri, options)

    # ---- Orchestration ----
    def ensure_spark_job(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        from .orchestrator import ensure_spark_job

        return ensure_spark_job(self.env, cfg)

    def ensure_pipeline(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        from .orchestrator import ensure_pipeline

        return ensure_pipeline(self.env, cfg)
