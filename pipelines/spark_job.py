import sys, yaml
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

cfg_path, env_path = sys.argv[1], sys.argv[2]
cfg = yaml.safe_load(open(cfg_path, 'r', encoding='utf-8'))
env = yaml.safe_load(open(env_path, 'r', encoding='utf-8'))

spark = (SparkSession.builder
    .appName(f"cfg-pipeline::{cfg['id']}")
    .config("spark.sql.session.timeZone", env.get('timezone','UTC'))
    .getOrCreate())

src = cfg['source']
fmt = src['input_format']
reader = spark.read.options(**src.get('options', {}))

if fmt == 'csv':
    df = reader.csv(src['path'])
elif fmt in ('json', 'jsonl'):
    df = reader.json(src['path'])
elif fmt == 'parquet':
    df = reader.parquet(src['path'])
else:
    raise ValueError(f"Formato no soportado: {fmt}")

std = cfg.get('standardization', {})
for r in std.get('rename', []):
    df = df.withColumnRenamed(r['from'], r['to'])

for c in std.get('casts', []):
    col, to = c['column'], c['to']
    df = df.withColumn(col, F.col(col).cast(to))

for d in std.get('defaults', []):
    col, val = d['column'], d['value']
    df = df.withColumn(col, F.when(F.col(col).isNull(), F.lit(val)).otherwise(F.col(col)))

if 'deduplicate' in std:
    key = std['deduplicate']['key']
    order = std['deduplicate']['order_by']
    order_expr = [F.col(o.split()[0]).desc() if 'desc' in o else F.col(o.split()[0]).asc() for o in order]
    w = Window.partitionBy(*key).orderBy(*order_expr)
    df = df.withColumn("_rn", F.row_number().over(w)).where("_rn=1").drop("_rn")

if 'payment_date' in df.columns:
    df = df.withColumn('year', F.year(F.col('payment_date'))).withColumn('month', F.month(F.col('payment_date')))

run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
df = df.withColumn('_run_id', F.lit(run_id)).withColumn('_ingestion_ts', F.current_timestamp())

out = cfg['output']['silver']
(df.write
  .format(out.get('format','parquet'))
  .mode('append')
  .partitionBy(*out.get('partition_by', []))
  .option('mergeSchema', str(out.get('merge_schema', True)).lower())
  .save(out['path']))

print(f"OK :: wrote to {out['path']}")
