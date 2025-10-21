from typing import Dict, List, Tuple, Optional
from pyspark.sql import DataFrame, functions as F
from pipelines.utils.logger import get_logger


def apply_quality(df: DataFrame, rules: List[Dict], quarantine_path: Optional[str], run_id: str):
    """Apply quality rules with quarantine, drop, warn, and fail semantics.

    Args:
        df: Input DataFrame
        rules: List of rule dicts with keys: name, expr, on_fail
        quarantine_path: Optional S3A/MinIO path to write bad records
        run_id: Current pipeline run identifier
    Returns:
        Tuple[DataFrame, Optional[DataFrame], Dict[str,int]] -> (good_df, bad_df, stats)
    """
    logger = get_logger("validation.quality", run_id)
    if not rules:
        return df, None, {}

    quarantine_rules: List[Tuple[str,str]] = []
    hard_fail: List[Tuple[str,str]] = []
    drops: List[str] = []
    warns: List[str] = []

    for r in rules:
        expr = r.get("expr") or r.get("filter")
        on_fail = (r.get("on_fail") or r.get("action") or "quarantine").lower()
        name = r.get("name", "rule")
        cond_bad = f"NOT ({expr})"
        if on_fail == "quarantine":
            quarantine_rules.append((name, cond_bad))
        elif on_fail == "fail":
            hard_fail.append((name, cond_bad))
        elif on_fail == "drop":
            drops.append(cond_bad)
        elif on_fail == "warn":
            warns.append(cond_bad)
        else:
            quarantine_rules.append((name, cond_bad))

    # Hard fails: if any record violates, abort
    for name, cond in hard_fail:
        if df.filter(cond).limit(1).count() > 0:
            raise ValueError(f"[quality][FAIL] Regla '{name}' violada.")

    # Build per-rule failure flags once
    bad_df = None
    if quarantine_rules:
        temp_df = df
        for rule_name, cond in quarantine_rules:
            temp_df = temp_df.withColumn(f"_fails_{rule_name}", F.expr(cond))
        any_fail_conditions = [f"_fails_{rule_name}" for rule_name, _ in quarantine_rules]
        any_fail_expr = " OR ".join(any_fail_conditions)
        bad_df = temp_df.filter(any_fail_expr)

        if bad_df.count() > 0:
            failed_rules_cases = [f"CASE WHEN _fails_{rule_name} THEN '{rule_name}' END" for rule_name, _ in quarantine_rules]
            failed_rules_expr = f"array_join(filter(array({', '.join(failed_rules_cases)}), x -> x IS NOT NULL), ', ')"
            bad_df = bad_df.withColumn('_failed_rules', F.expr(failed_rules_expr))
            for rule_name, _ in quarantine_rules:
                bad_df = bad_df.drop(f"_fails_{rule_name}")
            quarantine_count = bad_df.count()
            stats = {"quarantine_count": quarantine_count}
            all_bad_conditions = " OR ".join([f"({cond})" for _, cond in quarantine_rules])
            df = df.filter(f"NOT ({all_bad_conditions})")
        else:
            stats = {"quarantine_count": 0}
    else:
        stats = {}

    # Drops
    if drops:
        to_drop = " OR ".join([f"({c})" for c in drops])
        dropped_count = df.filter(to_drop).count()
        df = df.filter(f"NOT ({to_drop})")
        stats["dropped_count"] = dropped_count

    # Warns (only count)
    if warns:
        any_warn = " OR ".join([f"({c})" for c in warns])
        stats["warn_count"] = df.filter(any_warn).count()

    # Write quarantine if needed
    if bad_df is not None and quarantine_path and stats.get('quarantine_count', 0) > 0:
        (bad_df
          .withColumn('_quarantine_reason', F.concat(F.lit('rules_failed: '), F.col('_failed_rules')))
          .withColumn('_run_id', F.lit(run_id))
          .withColumn('_ingestion_ts', F.current_timestamp())
          .drop('_failed_rules')
          .write.mode('append').parquet(quarantine_path))
        logger.info(f"[quality] Quarantine -> {quarantine_path} rows={stats.get('quarantine_count',0)}")
    elif quarantine_rules and quarantine_path:
        logger.info("[quality] No quarantine needed - all records passed validation")

    return df, bad_df, stats