"""Storage abstraction contracts used by the orchestration core."""

from __future__ import annotations

from typing import Protocol


class StoragePort(Protocol):
    """Basic contract for reading configuration assets."""

    def read_text(self, uri: str) -> str:
        """Return the textual contents for ``uri``."""

    def exists(self, uri: str) -> bool:
        """Return whether the provided ``uri`` can be resolved."""


__all__ = ["StoragePort"]
