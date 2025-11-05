"""Cache configurable para datasets intermedios."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame

from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)


def _ttl_to_seconds(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        if value.endswith("ms"):
            return float(value[:-2]) / 1000
        if value.endswith("s"):
            return float(value[:-1])
        if value.endswith("m"):
            return float(value[:-1]) * 60
        if value.endswith("h"):
            return float(value[:-1]) * 3600
        if value.endswith("d"):
            return float(value[:-1]) * 86400
        return float(value)
    except ValueError:
        LOGGER.warning("TTL inválido: %s", value)
        return 0.0


def _cache_path(cfg: dict[str, Any], ctx: dict[str, Any]) -> Path:
    base = cfg.get("storage") or ctx.get("default_storage") or "/tmp/datacore-cache"
    signature = ctx.get("signature") or "default"
    digest = hashlib.sha1(signature.encode("utf-8")).hexdigest()
    dataset = ctx.get("dataset", "dataset")
    return Path(base) / dataset / f"{digest}.parquet"


def _is_valid(path: Path, ttl_seconds: float) -> bool:
    if ttl_seconds <= 0:
        return False
    if not path.exists():
        return False
    modified = path.stat().st_mtime
    age = time.time() - modified
    return age <= ttl_seconds


def with_cache(df: DataFrame, cfg: dict[str, Any] | None, ctx: dict[str, Any]) -> DataFrame:
    if not cfg or not cfg.get("enabled"):
        return df
    ttl_seconds = _ttl_to_seconds(cfg.get("ttl"))
    path = _cache_path(cfg, ctx)
    path.parent.mkdir(parents=True, exist_ok=True)
    if _is_valid(path, ttl_seconds):
        LOGGER.info("Recuperando cache para %s desde %s", ctx.get("dataset"), path)
        return df.sparkSession.read.parquet(str(path))

    LOGGER.info("Materializando cache para %s en %s", ctx.get("dataset"), path)
    df.write.mode("overwrite").parquet(str(path))
    metadata = {
        "generated_at": time.time(),
        "rows": df.count(),
        "signature": ctx.get("signature"),
    }
    path.with_suffix(".json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return df.sparkSession.read.parquet(str(path))


__all__ = ["with_cache"]
