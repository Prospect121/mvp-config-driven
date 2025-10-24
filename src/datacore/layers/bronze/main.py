from __future__ import annotations

from typing import Any, Dict

from datacore.context import build_context, ensure_spark, stop_spark, PipelineContext
from datacore.pipeline.utils import run_bronze_stage
from datacore.layers.raw.main import execute as execute_raw


def execute(context: PipelineContext, spark, df=None):
    if df is None:
        df = execute_raw(context, spark)
    return run_bronze_stage(context.dataset_cfg, context.env_cfg, spark, df)


def run(cfg: Dict[str, Any]) -> Any:
    context = build_context(cfg)
    if context.dry_run:
        print("[bronze] Dry run requested - skipping execution")
        return None

    try:
        spark = ensure_spark(context)
        return execute(context, spark)
    finally:
        stop_spark(context)
