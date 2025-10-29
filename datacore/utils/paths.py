"""Normalización de URIs."""

from __future__ import annotations

from urllib.parse import urlparse


def normalize_uri(uri: str) -> str:
    """Normaliza un URI de almacenamiento para Spark."""
    parsed = urlparse(uri)
    if not parsed.scheme:
        # Asumir ruta local
        return f"file://{uri}"
    if parsed.scheme in {"s3", "s3a"}:
        return uri.replace("s3://", "s3a://", 1)
    if parsed.scheme in {"abfs", "abfss"}:
        return uri.replace("abfs://", "abfss://", 1)
    if parsed.scheme == "gs":
        return uri
    return uri
