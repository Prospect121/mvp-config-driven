from typing import Dict
from pyspark.sql import DataFrame, functions as F
from pipelines.common import safe_cast  # reuse common helper via package import
from pipelines.udf_catalog import get_udf


def apply_sql_transforms(df: DataFrame, transforms_cfg: Dict) -> DataFrame:
    """Apply declarative SQL transforms.

    Compatible with the prior format used by transforms.yml, including raw SQL statements.
    """
    if not transforms_cfg:
        return df

    transforms_list = transforms_cfg.get('transforms') or transforms_cfg.get('sql') or []
    if not transforms_list:
        return df

    default_on_error = (transforms_cfg.get('on_error') or '').lower() or None

    for t in transforms_list:
        if not isinstance(t, dict):
            print(f"[transforms] Ignorando entrada no soportada: {t}")
            continue

        statement = t.get('statement')
        target = t.get('target_column') or t.get('name')
        expr = t.get('expr')
        mode = (t.get('mode') or 'create').lower()
        cast_type = t.get('type')
        on_error = (t.get('on_error') or default_on_error)

        if statement:
            temp_view = '__df__'
            try:
                df.createOrReplaceTempView(temp_view)
                df = df.sparkSession.sql(statement.replace('__df__', temp_view))
                print(f"[transforms] Applied statement: {statement}")
            finally:
                try:
                    df.sparkSession.catalog.dropTempView(temp_view)
                except Exception:
                    pass
            continue

        if not target or not expr:
            print(f"[transforms] Entrada inválida, falta target/expr: {t}")
            continue

        try:
            target_exists = target in df.columns
            if mode == 'create' and target_exists:
                print(f"[transforms] Skipped create for existing column '{target}'")
                continue

            df = df.withColumn(target, F.expr(expr))
            if cast_type:
                df = safe_cast(df, target, cast_type, on_error=on_error)
            print(f"[transforms] Applied expr to '{target}': {expr} type={cast_type or 'auto'}")
        except Exception as e:
            msg = f"[transforms] Error applying expr to '{target}': {e}"
            if (on_error or '').lower() == 'null':
                ttype = cast_type or 'string'
                df = df.withColumn(target, F.lit(None).cast(ttype))
                print(msg + f" -> set NULL ({ttype})")
            elif (on_error or '').lower() == 'skip':
                print(msg + ' -> skipped')
            else:
                print(msg)
                raise

    return df

def apply_udf_transforms(df: DataFrame, transforms_cfg: Dict) -> DataFrame:
    """Apply UDF-based transforms using the udf_catalog."""
    if not transforms_cfg:
        return df
    udf_list = transforms_cfg.get('udf') or []
    if not udf_list:
        return df
    default_on_error = (transforms_cfg.get('on_error') or '').lower() or None

    for t in udf_list:
        if not isinstance(t, dict):
            print(f"[udf] Ignorando entrada no soportada: {t}")
            continue
        target = t.get('target_column') or t.get('name')
        func_name = t.get('function')
        args = t.get('args', [])
        mode = (t.get('mode') or 'create').lower()
        cast_type = t.get('type')
        on_error = (t.get('on_error') or default_on_error)
        if not target or not func_name:
            print(f"[udf] Entrada inválida, falta target/function: {t}")
            continue
        udf_func = get_udf(func_name)
        if not udf_func:
            print(f"[udf] UDF no encontrada en catálogo: {func_name}")
            continue
        try:
            target_exists = target in df.columns
            if mode == 'create' and target_exists:
                print(f"[udf] Skipped create for existing column '{target}'")
                continue
            cols = []
            for a in args:
                if isinstance(a, str) and a in df.columns:
                    cols.append(F.col(a))
                else:
                    cols.append(F.lit(a))
            df = df.withColumn(target, udf_func(*cols))
            if cast_type:
                df = safe_cast(df, target, cast_type, on_error=on_error)
            print(f"[udf] Applied UDF '{func_name}' to '{target}' args={args} type={cast_type or 'auto'}")
        except Exception as e:
            msg = f"[udf] Error applying UDF '{func_name}' to '{target}': {e}"
            if (on_error or '').lower() == 'null':
                ttype = cast_type or 'string'
                df = df.withColumn(target, F.lit(None).cast(ttype))
                print(msg + f" -> set NULL ({ttype})")
            elif (on_error or '').lower() == 'skip':
                print(msg + " -> skipped")
            else:
                print(msg)
                raise
    return df