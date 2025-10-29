"""Job backend orchestration contracts."""

from __future__ import annotations

from typing import Any, Protocol


class JobBackendPort(Protocol):
    """Execution backend contract used by orchestrated layers."""

    def ensure_engine(self, context: object) -> Any:
        """Ensure an execution engine instance is available and return it."""

    def stop_engine(self, context: object) -> None:
        """Stop the execution engine if it was created."""


__all__ = ["JobBackendPort"]
