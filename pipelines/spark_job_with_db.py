from __future__ import annotations

import sys
from typing import Dict, Optional

from datacore.context import build_context, ensure_spark, stop_spark, PipelineContext
from datacore.layers.raw import main as raw_main
from datacore.layers.bronze import main as bronze_main
from datacore.layers.silver import main as silver_main
from datacore.layers.gold import main as gold_main


USAGE = "Usage: python spark_job_with_db.py <dataset_config> <env_config> [db_config] [environment]"


def _build_runtime_config(args: list[str]) -> Dict[str, Optional[str]]:
    cfg: Dict[str, Optional[str]] = {
        "dataset_config": args[0],
        "environment_config": args[1],
    }
    if len(args) > 2:
        cfg["database_config"] = args[2]
    if len(args) > 3:
        cfg["environment"] = args[3]
    return cfg


def _log_failure(context: PipelineContext, error: Exception) -> None:
    if context.execution_id and context.db_manager:
        try:
            context.db_manager.log_pipeline_execution(
                dataset_name=context.dataset_cfg.get('id'),
                status='failed',
                error_message=str(error),
                execution_id=context.execution_id,
            )
            print(f"[metadata] Pipeline execution failed: {context.execution_id}")
        except Exception as log_error:  # pragma: no cover - defensive logging
            print(f"[metadata] Warning: Failed to log pipeline failure: {log_error}")


def main() -> None:
    if len(sys.argv) < 3:
        print(USAGE)
        sys.exit(1)

    runtime_cfg = _build_runtime_config(sys.argv[1:])
    context = build_context(runtime_cfg)

    if context.dry_run:
        print("[pipeline] Dry run configuration detected; skipping execution")
        return

    try:
        spark = ensure_spark(context)
        df = raw_main.execute(context, spark)
        df = bronze_main.execute(context, spark, df)
        df = silver_main.execute(context, spark, df)
        gold_main.execute(context, spark, df)
    except Exception as exc:
        _log_failure(context, exc)
        raise
    finally:
        stop_spark(context)


if __name__ == "__main__":
    main()
