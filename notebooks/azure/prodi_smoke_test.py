# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Prueba de humo de prodi – Azure Databricks
# MAGIC
# MAGIC Este cuaderno prepara un dataset de ejemplo y ejecuta el pipeline `prodi` RAW→BRONZE→SILVER usando almacenamiento ADLS Gen2. Asegúrate de que el clúster del job exponga las siguientes variables de entorno (el job de Terraform ya las configura):
# MAGIC
# MAGIC * `AZURE_STORAGE_ACCOUNT_NAME`
# MAGIC * `AZURE_STORAGE_ACCOUNT_KEY`
# MAGIC * `TZ=America/Bogota`

# COMMAND ----------
# MAGIC %pip install --quiet "git+https://github.com/Prospect121/mvp-config-driven@feature/main-codex"

# COMMAND ----------
import importlib
import os
import subprocess
import sys

try:
    importlib.import_module("datacore")
    importlib.import_module("prodi")
except ModuleNotFoundError:
    wheels_dir = "dbfs:/FileStore/prodi/wheels"
    try:
        wheel_files = [f.path for f in dbutils.fs.ls(wheels_dir) if f.path.endswith(".whl")]
    except Exception as err:  # noqa: BLE001
        raise RuntimeError("No es posible acceder al directorio de wheels de respaldo") from err

    if not wheel_files:
        raise RuntimeError("El paquete datacore/prodi no está instalado y no se encontró ningún wheel en /FileStore/prodi/wheels")

    for wheel in wheel_files:
        wheel_local_path = wheel.replace("dbfs:/", "/dbfs/")
        subprocess.check_call([sys.executable, "-m", "pip", "install", wheel_local_path])

# COMMAND ----------
account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

assert account and key, "Faltan las variables de entorno AZURE_STORAGE_ACCOUNT_NAME/KEY"

spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)

containers = {
    "raw": f"abfss://raw@{account}.dfs.core.windows.net",
    "bronze": f"abfss://bronze@{account}.dfs.core.windows.net",
    "silver": f"abfss://silver@{account}.dfs.core.windows.net",
    "gold": f"abfss://gold@{account}.dfs.core.windows.net",
    "checkpoints": f"abfss://checkpoints@{account}.dfs.core.windows.net",
}

display(containers)

# COMMAND ----------
from pyspark.sql import functions as F

sample_df = spark.createDataFrame(
    [
        ("C001", "SMB", "ACTIVE", "2024-01-10T00:00:00Z"),
        ("C002", "ENT", "INACTIVE", "2024-06-01T12:30:00Z"),
        ("C003", "SMB", "ACTIVE", "2025-01-01T08:00:00Z"),
    ],
    ["CUSTOMER_ID", "SEGMENT", "STATUS", "SIGNUP_TS"],
).withColumn("SIGNUP_TS", F.to_timestamp("SIGNUP_TS"))

raw_path = f"{containers['raw']}/toy/customers.csv"
(
    sample_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(raw_path)
)

print(f"Dataset RAW escrito en {raw_path}")

# COMMAND ----------
import textwrap

dbfs_cfg_root = "dbfs:/FileStore/prodi/cfg"
dbutils.fs.mkdirs(dbfs_cfg_root)

def put_dbfs_text(path: str, text: str) -> None:
    dbutils.fs.put(path, textwrap.dedent(text).strip() + "\n", True)

env_yml = """
    timezone: America/Bogota
    storage:
      abfss:
        credentials:
          account_name_env: AZURE_STORAGE_ACCOUNT_NAME
          account_key_env: AZURE_STORAGE_ACCOUNT_KEY
        storage_options: {}
"""

raw_yml = f"""
    dry_run: false
    environment: development
    environment_config: {dbfs_cfg_root}/env.yml
    compute:
      engine: spark
    io:
      source:
        path: {raw_path}
        format: csv
        options:
          header: true
          inferSchema: true
      sink:
        format: delta
        uris:
          abfss: {containers['bronze']}/tables/customers_raw/
"""

bronze_yml = f"""
    dry_run: false
    environment: development
    environment_config: {dbfs_cfg_root}/env.yml
    compute:
      engine: spark
    io:
      source:
        path: {containers['bronze']}/tables/customers_raw/
        format: delta
      sink:
        format: delta
        uris:
          abfss: {containers['silver']}/tables/customers/
    transform:
      steps:
        - name: normalize_columns
          params:
            mappings:
              CUSTOMER_ID: customer_id
              SEGMENT: segment
              STATUS: status
      standardization:
        timestamp_columns:
          - signup_ts
    dq:
      expectations:
        min_row_count: 1
        expect_columns_to_exist:
          - customer_id
          - segment
          - status
      fail_on_error: true
"""

silver_yml = f"""
    dry_run: false
    environment: development
    environment_config: {dbfs_cfg_root}/env.yml
    compute:
      engine: spark
    io:
      source:
        path: {containers['silver']}/tables/customers/
        format: delta
      sink:
        format: delta
        uris:
          abfss: {containers['gold']}/marts/customers/
    transform:
      steps:
        - name: enrich_with_segments
          params:
            lookup_view: inline
    dq:
      expectations:
        expect_columns_to_exist:
          - customer_id
          - segment
          - status
      fail_on_error: true
"""

put_dbfs_text(f"{dbfs_cfg_root}/env.yml", env_yml)
put_dbfs_text(f"{dbfs_cfg_root}/raw.yml", raw_yml)
put_dbfs_text(f"{dbfs_cfg_root}/bronze.yml", bronze_yml)
put_dbfs_text(f"{dbfs_cfg_root}/silver.yml", silver_yml)

display(dbutils.fs.ls(dbfs_cfg_root))

# COMMAND ----------
import shlex


def run(cmd: str):
    print(f"> {cmd}")
    completed = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
    print(completed.stdout)
    if completed.returncode != 0:
        print(completed.stderr, file=sys.stderr)
        raise RuntimeError(f"Falló: {cmd}")
    return completed

# COMMAND ----------
run("prodi validate -c /dbfs/FileStore/prodi/cfg/raw.yml")
run("prodi run-layer raw -c /dbfs/FileStore/prodi/cfg/raw.yml")

run("prodi validate -c /dbfs/FileStore/prodi/cfg/bronze.yml")
run("prodi run-layer bronze -c /dbfs/FileStore/prodi/cfg/bronze.yml")

run("prodi validate -c /dbfs/FileStore/prodi/cfg/silver.yml")
run("prodi run-layer silver -c /dbfs/FileStore/prodi/cfg/silver.yml")

# COMMAND ----------
result_path = f"{containers['gold']}/marts/customers/"

try:
    final_df = spark.read.format("delta").load(result_path)
except Exception:
    # Cambio a SILVER si la canalización aún no genera GOLD
    result_path = f"{containers['silver']}/tables/customers/"
    final_df = spark.read.format("delta").load(result_path)

display(final_df.limit(10))
print(f"Rows: {final_df.count()}")
print(f"Validated path: {result_path}")

# COMMAND ----------
# Celda de marcador para emitir lineage, métricas o dq si se habilita en env.yml
lineage_root = "dbfs:/FileStore/prodi/_lineage"
metrics_root = "dbfs:/FileStore/prodi/_metrics"


def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:  # noqa: BLE001
        return False


artifacts = {}
if path_exists(lineage_root):
    artifacts["lineage"] = lineage_root
if path_exists(metrics_root):
    artifacts["metrics"] = metrics_root

if artifacts:
    display(artifacts)
else:
    print("No se detectaron artefactos de lineage/métricas.")
