from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from datacore.context import build_context, ensure_spark, stop_spark, PipelineContext
from datacore.layers.silver.main import execute as execute_silver
from datacore.pipeline.utils import (
    apply_gold_transformations,
    write_to_gold_bucket,
    write_to_gold_database,
)


def execute(context: PipelineContext, spark, df=None):
    if df is None:
        df = execute_silver(context, spark)

    cfg = context.dataset_cfg
    env = context.env_cfg
    db_manager = context.db_manager
    execution_id = context.execution_id
    gold_config = cfg.get('output', {}).get('gold', {})

    gold_db_cfg = gold_config.get('database', {})
    gold_db_enabled = gold_config.get('enabled', False) or gold_db_cfg.get('enabled', False)
    gold_bucket_enabled = gold_config.get('bucket', {}).get('enabled', False)

    if not (gold_db_enabled or gold_bucket_enabled):
        print('[gold] Capa Gold deshabilitada, omitiendo...')
        print('[pipeline] Pipeline completado exitosamente (solo Silver layer)')
        if db_manager and execution_id:
            try:
                db_manager.log_pipeline_execution(
                    dataset_name=cfg.get('id'),
                    status='completed',
                    execution_id=execution_id,
                )
                print(f"[metadata] Pipeline execution completed: {execution_id}")
            except Exception as exc:
                print(f"[metadata] Warning: Failed to log pipeline completion: {exc}")
        return df

    dataset_id = cfg.get('id')
    schema_path = cfg.get('schema', {}).get('ref')
    print(f"[gold] Starting Gold layer processing for dataset: {dataset_id}")

    effective_table_settings = dict(context.table_settings) if context.table_settings else {}
    for key in ('write_mode', 'default_write_mode'):
        value = gold_config.get(key)
        if value:
            effective_table_settings['default_write_mode'] = value
    if gold_config.get('upsert_keys'):
        effective_table_settings['upsert_keys'] = gold_config['upsert_keys']
    if gold_config.get('table_prefix'):
        effective_table_settings['table_prefix'] = gold_config['table_prefix']
    if gold_config.get('table_suffix'):
        effective_table_settings['table_suffix'] = gold_config['table_suffix']

    if gold_db_cfg.get('write_mode'):
        effective_table_settings['default_write_mode'] = gold_db_cfg['write_mode']
    if gold_db_cfg.get('upsert_keys'):
        effective_table_settings['upsert_keys'] = gold_db_cfg['upsert_keys']
    if gold_db_cfg.get('table_prefix'):
        effective_table_settings['table_prefix'] = gold_db_cfg['table_prefix']
    if gold_db_cfg.get('table_suffix'):
        effective_table_settings['table_suffix'] = gold_db_cfg['table_suffix']

    gold_df = apply_gold_transformations(df, gold_config, effective_table_settings)

    db_success = False
    if gold_db_enabled:
        if db_manager and schema_path:
            db_success = write_to_gold_database(
                df=gold_df,
                dataset_id=dataset_id,
                schema_path=schema_path,
                db_manager=db_manager,
                table_settings=effective_table_settings,
            )
            if db_success and db_manager:
                try:
                    gold_version = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                    db_manager.log_dataset_version(
                        dataset_name=f"{dataset_id}_gold",
                        version=gold_version,
                        schema_path=schema_path,
                        record_count=gold_df.count(),
                    )
                    print(f"[metadata] Gold dataset version logged: {gold_version}")
                except Exception as exc:
                    print(f"[metadata] Warning: Failed to log gold dataset version: {exc}")
        elif gold_db_enabled and not db_manager:
            print('[gold] Advertencia: Gold (DB) habilitado pero no hay db_manager disponible')

    bucket_success = False
    if gold_bucket_enabled:
        bucket_cfg = gold_config.get('bucket', {})
        try:
            bucket_success = write_to_gold_bucket(
                df=gold_df,
                dataset_id=dataset_id,
                schema_path=schema_path,
                bucket_cfg=bucket_cfg,
                env=env,
                spark=spark,
            )
        except Exception as exc:
            print(f"[gold][bucket] Error: {exc}")

    if db_manager and execution_id:
        try:
            status = 'completed' if ((not gold_db_enabled or db_success) and (not gold_bucket_enabled or bucket_success)) else 'failed'
            db_manager.log_pipeline_execution(
                dataset_name=cfg.get('id'),
                status=status,
                execution_id=execution_id,
            )
            print(f"[metadata] Pipeline execution {status}: {execution_id}")
            if status == 'completed':
                db_manager.log_pipeline_execution(
                    dataset_name=cfg.get('id'),
                    status='completed',
                    execution_id=execution_id,
                )
                print(f"[metadata] Pipeline execution completed: {execution_id}")
        except Exception as exc:
            print(f"[metadata] Warning: Failed to log pipeline status: {exc}")

    print('[pipeline] Pipeline completed successfully.')
    return gold_df


def run(cfg: Dict[str, Any]) -> Any:
    context = build_context(cfg)
    if context.dry_run:
        print('[gold] Dry run requested - skipping execution')
        return None

    try:
        spark = ensure_spark(context)
        return execute(context, spark)
    finally:
        stop_spark(context)
