from __future__ import annotations

from typing import Any, Dict

from datacore.context import build_context, ensure_spark, stop_spark, PipelineContext
from datacore.pipeline.utils import run_raw_sources


def execute(context: PipelineContext, spark) -> Any:
    return run_raw_sources(context.dataset_cfg, spark, context.env_cfg)


def run(cfg: Dict[str, Any]) -> Any:
    context = build_context(cfg)
    if context.dry_run:
        print("[raw] Dry run requested - skipping execution")
        return None

    try:
        spark = ensure_spark(context)
        return execute(context, spark)
    finally:
        stop_spark(context)
