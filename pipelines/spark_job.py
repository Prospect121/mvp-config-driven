import sys, os, yaml, json
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Importar funciones comunes
from common import norm_type, parse_order, safe_cast
from datacore.io import build_storage_adapter


# -------- helpers de esquema --------
def load_json_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def spark_type_from_json(t):
    # t puede ser list (union con null) o string
    if isinstance(t, list):
        t = [x for x in t if x != "null"]
        t = t[0] if t else "string"
    m = {
        "string": "string",
        "number": "double",
        "integer": "bigint",
        "boolean": "boolean",
        "object": "string",
        "array": "string",
    }
    return m.get(t, "string")

def enforce_schema(df, jsch, mode="strict"):
    props = jsch.get("properties", {}) or {}
    required = set(jsch.get("required", []))
    defined_cols = list(props.keys())  # mantiene orden del JSON Schema
    df_cols = set(df.columns)

    missing = required - df_cols
    extra = df_cols - set(defined_cols)

    if missing:
        msg = f"Columnas requeridas faltantes: {sorted(missing)}"
        if mode == "strict":
            raise ValueError(msg)
        print("[schema][WARN]", msg)

    # Crear columnas ausentes con tipo aproximado desde JSON Schema
    for c in (set(defined_cols) - df_cols):
        df = df.withColumn(c, F.lit(None).cast(spark_type_from_json(props[c].get("type"))))

    # Si strict, limita al orden exacto del schema
    if mode == "strict":
        keep_ordered = [c for c in defined_cols if c in df.columns]
        df = df.select(*keep_ordered)
    else:
        if extra and jsch.get("additionalProperties", True) is False:
            print("[schema][WARN] Columnas adicionales presentes:", sorted(extra))

    # Casteo automático por formato si aplica (date-time -> timestamp)
    # Nota: si ya casteaste en 'casts', esto no hace daño (Spark re-castea a mismo tipo).
    for c in defined_cols:
        p = props.get(c) or {}
        fmt = (p.get("format") or "").lower()
        # soporta JSON Schema: {"type":["string","null"], "format":"date-time"}
        t = p.get("type")
        t_non_null = None
        if isinstance(t, list):
            t_non_null = [x for x in t if x != "null"]
            t_non_null = t_non_null[0] if t_non_null else None
        else:
            t_non_null = t

        if fmt in ("date-time", "datetime") and c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))

    return df


# -------- helpers de calidad --------
def load_expectations(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def apply_quality(df, rules, quarantine_path, run_id):
    if not rules: return df, None, {}

    quarantine_rules, hard_fail, drops, warns = [], [], [], []
    for r in rules:
        expr = r["expr"]
        on_fail = r.get("on_fail","quarantine").lower()
        name = r.get("name","rule")
        cond_bad = f"NOT ({expr})"
        if on_fail == "quarantine": quarantine_rules.append((name, cond_bad))
        elif on_fail == "fail":     hard_fail.append((name, cond_bad))
        elif on_fail == "drop":     drops.append(cond_bad)
        elif on_fail == "warn":     warns.append(cond_bad)
        else:                       quarantine_rules.append((name, cond_bad))

    stats = {}
    for name, cond in hard_fail:
        if df.filter(cond).limit(1).count() > 0:
            raise ValueError(f"[quality][FAIL] Regla '{name}' violada.")

    # Procesar reglas de cuarentena acumulando todas las fallas por registro
    bad_df = None
    if quarantine_rules:
        # Crear una columna para cada regla que indique si falla (True/False)
        temp_df = df
        for rule_name, cond in quarantine_rules:
            temp_df = temp_df.withColumn(f"_fails_{rule_name}", F.expr(cond))
        
        # Crear una condición que identifique registros que fallan al menos una regla
        any_fail_conditions = [f"_fails_{rule_name}" for rule_name, _ in quarantine_rules]
        any_fail_expr = " OR ".join(any_fail_conditions)
        
        # Filtrar registros que fallan al menos una regla
        bad_df = temp_df.filter(any_fail_expr)
        
        if bad_df.count() > 0:
            # Crear una lista de reglas fallidas por registro
            failed_rules_expr = "CASE "
            for i, (rule_name, _) in enumerate(quarantine_rules):
                if i == 0:
                    failed_rules_expr += f"WHEN _fails_{rule_name} THEN '{rule_name}'"
                else:
                    failed_rules_expr += f" WHEN _fails_{rule_name} THEN CONCAT_WS(', ', CASE WHEN _fails_{rule_name} THEN '{rule_name}' END"
                    # Agregar reglas anteriores si también fallan
                    for j in range(i):
                        prev_rule_name, _ = quarantine_rules[j]
                        failed_rules_expr += f", CASE WHEN _fails_{prev_rule_name} THEN '{prev_rule_name}' END"
                    failed_rules_expr += ")"
            failed_rules_expr += " END"
            
            # Simplificar: usar array y array_join para concatenar todas las reglas fallidas
            failed_rules_cases = []
            for rule_name, _ in quarantine_rules:
                failed_rules_cases.append(f"CASE WHEN _fails_{rule_name} THEN '{rule_name}' END")
            
            failed_rules_expr = f"array_join(filter(array({', '.join(failed_rules_cases)}), x -> x IS NOT NULL), ', ')"
            
            bad_df = bad_df.withColumn('_failed_rules', F.expr(failed_rules_expr))
            
            # Remover las columnas temporales de fallas
            for rule_name, _ in quarantine_rules:
                bad_df = bad_df.drop(f"_fails_{rule_name}")
            
            stats["quarantine_count"] = bad_df.count()
            
            # Filtrar registros buenos (que no fallan ninguna regla de cuarentena)
            all_bad_conditions = " OR ".join([f"({cond})" for _, cond in quarantine_rules])
            df = df.filter(f"NOT ({all_bad_conditions})")

    if drops:
        to_drop = " OR ".join([f"({c})" for c in drops])
        stats["dropped_count"] = df.filter(to_drop).count()
        df = df.filter(f"NOT ({to_drop})")

    if warns:
        any_warn = " OR ".join([f"({c})" for c in warns])
        stats["warn_count"] = df.filter(any_warn).count()

    if bad_df is not None and quarantine_path and stats.get('quarantine_count', 0) > 0:
        (bad_df
          .withColumn('_quarantine_reason', F.concat(F.lit('rules_failed: '), F.col('_failed_rules')))
          .withColumn('_run_id', F.lit(run_id))
          .withColumn('_ingestion_ts', F.current_timestamp())
          .drop('_failed_rules')  # Remover columna temporal
          .write.mode('append').parquet(quarantine_path))
        print(f"[quality] Quarantine -> {quarantine_path} rows={stats.get('quarantine_count',0)}")
    elif quarantine_rules and quarantine_path:
        print(f"[quality] No quarantine needed - all records passed validation")

    return df, bad_df, stats

# ===== main =====
cfg_path, env_path = sys.argv[1], sys.argv[2]
cfg = yaml.safe_load(open(cfg_path, 'r', encoding='utf-8'))
env = yaml.safe_load(open(env_path, 'r', encoding='utf-8'))

spark = (
    SparkSession.builder
    .appName(f"cfg-pipeline::{cfg['id']}")
    .config("spark.sql.session.timeZone", env.get('timezone','UTC'))
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

# S3A autoconfig se maneja desde el módulo común

src = cfg['source']
out = cfg['output']['silver']
build_storage_adapter(src.get('path'), env, src)
build_storage_adapter(out.get('path'), env, out)

reader = spark.read.options(**src.get('options', {}))
fmt = src['input_format']
if fmt == 'csv':
    df = reader.csv(src['path'])
elif fmt in ('json', 'jsonl'):
    df = reader.json(src['path'])
elif fmt == 'parquet':
    df = reader.parquet(src['path'])
else:
    raise ValueError(f"Formato no soportado: {fmt}")

# --- standardization.rename primero ---
std = cfg.get('standardization', {})
for r in std.get('rename', []) or []:
    if r['from'] in df.columns:
        df = df.withColumnRenamed(r['from'], r['to'])

# --- schema.enforce justo tras rename ---
if 'schema' in cfg and cfg['schema'].get('ref'):
    jsch = load_json_schema(cfg['schema']['ref'])
    mode = cfg['schema'].get('mode','strict').lower()
    df = enforce_schema(df, jsch, mode=mode)

# --- casts / defaults ---
for c in std.get('casts', []) or []:
    df = safe_cast(df, c['column'], c['to'], fmt=c.get('format_hint'), on_error=c.get('on_error','fail'))

for d in std.get('defaults', []) or []:
    col, val = d['column'], d['value']
    df = df.withColumn(col, F.when(F.col(col).isNull(), F.lit(val)).otherwise(F.col(col)))

# --- deduplicate ---
if 'deduplicate' in std:
    key = std['deduplicate']['key']
    order = parse_order(std['deduplicate'].get('order_by', []))
    w = Window.partitionBy(*key).orderBy(*order) if order else Window.partitionBy(*key)
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn")==1).drop("_rn")

# # --- particiones por fecha ---
# if 'payment_date' in df.columns:
#     df = df.withColumn('year', F.year('payment_date')).withColumn('month', F.month('payment_date'))

# --- particiones por configuración ---
# Si en la configuración se especifica `output.silver.partition_by`,
# generamos columnas de partición derivadas de una columna base.
# La columna base puede definirse como `output.silver.partition_from`.
# Si no se define, se usa `payment_date` si existe.
parts = out.get('partition_by', [])
if parts:
    base_col_name = out.get('partition_from')
    base_col = None
    if base_col_name and base_col_name in df.columns:
        base_col = F.col(base_col_name)

    # Crear columnas de partición comunes si faltan y hay columna base
    if base_col is not None:
        for p in parts:
            if p not in df.columns:
                lp = p.lower()
                if lp == 'year':
                    df = df.withColumn('year', F.year(base_col))
                elif lp == 'month':
                    df = df.withColumn('month', F.month(base_col))
                elif lp == 'day':
                    df = df.withColumn('day', F.dayofmonth(base_col))
                elif lp == 'date':
                    df = df.withColumn('date', F.to_date(base_col))
                # Otros nombres de partición se asumen ya presentes en el DF

# --- auditoría ---
run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
df = df.withColumn('_run_id', F.lit(run_id)).withColumn('_ingestion_ts', F.current_timestamp())

# --- quality ---
if 'quality' in cfg and cfg['quality'].get('expectations_ref'):
    q = load_expectations(cfg['quality']['expectations_ref'])
    rules = q.get('rules', [])
    quarantine_path = cfg['quality'].get('quarantine')
    build_storage_adapter(quarantine_path, env, quality_cfg)
    df, _, stats = apply_quality(df, rules, quarantine_path, run_id)
    print("[quality] stats:", stats)

# --- inspección ---
df.printSchema()
print("rows=", df.count())

# --- write ---
writer = (
    df.write
    .format(out.get('format','parquet'))
    .option('mergeSchema', str(out.get('merge_schema', True)).lower())
)

parts = out.get('partition_by', [])
if parts:
    writer = writer.partitionBy(*parts)

mode_cfg = (out.get('mode','append') or 'append').lower()
if mode_cfg == 'overwrite_dynamic':
    # gracias a spark.sql.sources.partitionOverwriteMode=dynamic
    writer.mode('overwrite').save(out['path'])
else:
    writer.mode('append').save(out['path'])


writer.save(out['path'])
print(f"OK :: wrote to {out['path']}")
