import sys, os, yaml, re, json
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# -------- helpers de tipos --------
DECIMAL_FULL = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
DECIMAL_PREC = re.compile(r"^decimal\(\s*(\d+)\s*\)$", re.IGNORECASE)
ALIASES = {
    "string":"string","str":"string","varchar":"string","char":"string",
    "int":"int","integer":"int","bigint":"bigint","long":"bigint",
    "double":"double","float":"double","boolean":"boolean","bool":"boolean",
    "date":"date","datetime":"timestamp","timestamp":"timestamp","timestamptz":"timestamp",
    "number":"double","numeric":"double","decimal":"decimal(38,18)"
}
def norm_type(t: str) -> str:
    raw = (t or "").strip().lower()
    m = DECIMAL_FULL.match(raw)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        if s > p: raise ValueError(f"decimal({p},{s}) inválido: scale>precision")
        return f"decimal({p},{s})"
    m = DECIMAL_PREC.match(raw)
    if m: return f"decimal({int(m.group(1))},0)"
    if raw in ALIASES: return ALIASES[raw]
    if raw.startswith("decimal(") and raw.endswith(")"):
        return raw
    raise ValueError(f"Tipo no reconocido: {t}")

def parse_order(exprs):
    out = []
    for e in (exprs or []):
        p = e.strip().split()
        col = p[0]; desc = len(p)>1 and p[1].lower()=="desc"
        out.append(F.col(col).desc() if desc else F.col(col).asc())
    return out

def safe_cast(df, col, target, fmt=None, on_error="fail"):
    t = norm_type(target)
    c = F.col(col)
    if t == "timestamp":
        newc = F.to_timestamp(c, fmt) if fmt else F.to_timestamp(c)
    elif t == "date":
        newc = F.to_date(c, fmt) if fmt else F.to_date(c)
    else:
        newc = c.cast(t)
    return df.withColumn(col, newc)  # on_error=null se comporta igual (Spark deja null si no parsea)

# -------- helpers de esquema --------
def load_json_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def spark_type_from_json(t):
    if isinstance(t, list):
        t = [x for x in t if x != "null"][0]
    m = {
        "string":"string", "number":"double", "integer":"bigint",
        "boolean":"boolean", "object":"string", "array":"string"
    }
    return m.get(t, "string")

def enforce_schema(df, jsch, mode="strict"):
    props = jsch.get("properties", {})
    required = set(jsch.get("required", []))
    defined_cols = list(props.keys())
    df_cols = set(df.columns)

    missing = required - df_cols
    extra = df_cols - set(defined_cols)

    if missing:
        msg = f"Columnas requeridas faltantes: {sorted(missing)}"
        if mode == "strict":
            raise ValueError(msg)
        print("[schema][WARN]", msg)

    to_create = (set(defined_cols) - df_cols)
    for c in to_create:
        df = df.withColumn(c, F.lit(None).cast(spark_type_from_json(props[c]["type"])))

    if extra and mode == "strict":
        keep = [c for c in defined_cols if c in df.columns]
        df = df.select(*keep)

    if jsch.get("additionalProperties", True) is False and extra and mode != "strict":
        print("[schema][WARN] Columnas adicionales presentes:", sorted(extra))

    return df

# -------- helpers de calidad --------
def load_expectations(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def apply_quality(df, rules, quarantine_path, run_id):
    if not rules: return df, None, {}

    bad_filters, hard_fail, drops, warns = [], [], [], []
    for r in rules:
        expr = r["expr"]
        on_fail = r.get("on_fail","quarantine").lower()
        name = r.get("name","rule")
        cond_bad = f"NOT ({expr})"
        if on_fail == "quarantine": bad_filters.append(cond_bad)
        elif on_fail == "fail":     hard_fail.append((name, cond_bad))
        elif on_fail == "drop":     drops.append(cond_bad)
        elif on_fail == "warn":     warns.append(cond_bad)
        else:                       bad_filters.append(cond_bad)

    stats = {}
    for name, cond in hard_fail:
        if df.filter(cond).limit(1).count() > 0:
            raise ValueError(f"[quality][FAIL] Regla '{name}' violada.")

    bad_df = None
    if bad_filters:
        any_bad = " OR ".join([f"({c})" for c in bad_filters])
        bad_df = df.filter(any_bad)
        stats["quarantine_count"] = bad_df.count()
        df = df.filter(f"NOT ({any_bad})")

    if drops:
        to_drop = " OR ".join([f"({c})" for c in drops])
        stats["dropped_count"] = df.filter(to_drop).count()
        df = df.filter(f"NOT ({to_drop})")

    if warns:
        any_warn = " OR ".join([f"({c})" for c in warns])
        stats["warn_count"] = df.filter(any_warn).count()

    if bad_df is not None and quarantine_path:
        (bad_df
          .withColumn('_quarantine_reason', F.lit('rules_failed'))
          .withColumn('_run_id', F.lit(run_id))
          .withColumn('_ingestion_ts', F.current_timestamp())
          .write.mode('append').parquet(quarantine_path))
        print(f"[quality] Quarantine -> {quarantine_path} rows={stats.get('quarantine_count',0)}")

    return df, bad_df, stats

# ===== main =====
cfg_path, env_path = sys.argv[1], sys.argv[2]
cfg = yaml.safe_load(open(cfg_path, 'r', encoding='utf-8'))
env = yaml.safe_load(open(env_path, 'r', encoding='utf-8'))

spark = (SparkSession.builder
    .appName(f"cfg-pipeline::{cfg['id']}")
    .config("spark.sql.session.timeZone", env.get('timezone','UTC'))
    .getOrCreate())

# S3A autoconfig si se usa s3a://
def maybe_config_s3a(path_like: str):
    if not path_like.startswith("s3a://"): return
    ak = os.getenv("MINIO_ROOT_USER") or os.getenv("AWS_ACCESS_KEY_ID")
    sk = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("AWS_SECRET_ACCESS_KEY")
    endpoint = env.get("s3a_endpoint") or os.getenv("S3A_ENDPOINT") or "http://minio:9000"
    if ak and sk:
        h = spark._jsc.hadoopConfiguration()
        h.set("fs.s3a.endpoint", endpoint)
        h.set("fs.s3a.path.style.access","true")
        h.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        h.set("fs.s3a.access.key", ak)
        h.set("fs.s3a.secret.key", sk)

src = cfg['source']
out = cfg['output']['silver']
maybe_config_s3a(src['path'])
maybe_config_s3a(out['path'])

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

# --- particiones por fecha ---
if 'payment_date' in df.columns:
    df = df.withColumn('year', F.year('payment_date')).withColumn('month', F.month('payment_date'))

# --- auditoría ---
run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
df = df.withColumn('_run_id', F.lit(run_id)).withColumn('_ingestion_ts', F.current_timestamp())

# --- quality ---
if 'quality' in cfg and cfg['quality'].get('expectations_ref'):
    q = load_expectations(cfg['quality']['expectations_ref'])
    rules = q.get('rules', [])
    quarantine_path = cfg['quality'].get('quarantine')
    maybe_config_s3a(quarantine_path or "")
    df, _, stats = apply_quality(df, rules, quarantine_path, run_id)
    print("[quality] stats:", stats)

# --- inspección ---
df.printSchema()
print("rows=", df.count())

# --- write ---
writer = (df.write
    .format(out.get('format','parquet'))
    .mode(out.get('mode','append'))
    .option('mergeSchema', str(out.get('merge_schema', True)).lower()))
parts = out.get('partition_by', [])
if parts: writer = writer.partitionBy(*parts)

writer.save(out['path'])
print(f"OK :: wrote to {out['path']}")
