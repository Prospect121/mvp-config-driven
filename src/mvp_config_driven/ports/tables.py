"""Table catalog and metadata orchestration contracts."""

from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, Tuple


class TablesPort(Protocol):
    """Interface used to interact with metadata catalogs and execution logs."""

    def load_metadata(self, path: str, environment: str) -> Tuple[Any, Dict[str, Any]]:
        """Load a database manager and associated table settings."""

    def log_pipeline_execution(
        self, manager: Any, *, dataset_name: str, pipeline_type: str, status: str
    ) -> Optional[str]:
        """Persist execution metadata and return an identifier when available."""


__all__ = ["TablesPort"]
