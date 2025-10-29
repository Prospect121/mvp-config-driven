"""Secrets manager orchestration contracts."""

from __future__ import annotations

from typing import Optional, Protocol


class SecretsPort(Protocol):
    """Minimal interface for retrieving secrets."""

    def get_secret(self, key: str) -> Optional[str]:
        """Return the secret identified by ``key`` when available."""


__all__ = ["SecretsPort"]
