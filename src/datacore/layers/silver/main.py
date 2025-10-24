from __future__ import annotations

from typing import Any, Dict

from datacore.context import build_context, ensure_spark, stop_spark, PipelineContext
from datacore.layers.bronze.main import execute as execute_bronze
from datacore.pipeline.utils import run_silver_stage


def execute(context: PipelineContext, spark, df=None):
    if df is None:
        df = execute_bronze(context, spark)
    return run_silver_stage(context.dataset_cfg, context.env_cfg, spark, df, context.db_manager)


def run(cfg: Dict[str, Any]) -> Any:
    context = build_context(cfg)
    if context.dry_run:
        print("[silver] Dry run requested - skipping execution")
        return None

    try:
        spark = ensure_spark(context)
        return execute(context, spark)
    finally:
        stop_spark(context)
