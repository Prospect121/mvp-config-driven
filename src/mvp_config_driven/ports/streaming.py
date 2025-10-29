"""Streaming orchestration contracts."""

from __future__ import annotations

from typing import Protocol


class StreamingPort(Protocol):
    """Interface exposed to streaming orchestrators."""

    def start(self, context: object) -> None:  # pragma: no cover - placeholder contract
        """Start streaming jobs for the provided ``context``."""

    def stop(self, context: object) -> None:  # pragma: no cover - placeholder contract
        """Stop streaming jobs for the provided ``context``."""


__all__ = ["StreamingPort"]
