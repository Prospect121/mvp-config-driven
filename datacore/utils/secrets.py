"""Gestión básica de secretos."""

from __future__ import annotations

import os


class SecretNotFoundError(RuntimeError):
    """Se lanza cuando un secreto no existe."""


def get_secret(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise SecretNotFoundError(f"No se encontró el secreto {name}")
    return value
