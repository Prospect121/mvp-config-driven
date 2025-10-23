# Ruta local al wheel
locals {
  wheel_source_path = coalesce(var.local_wheel_path, "${path.root}/../../dist/${var.package_wheel_filename}")
}

# Subir wheel a DBFS
resource "databricks_dbfs_file" "wheel" {
  source = local.wheel_source_path
  path   = "/FileStore/${var.package_wheel_filename}"
}

# Crear init script en DBFS para copiar runner a disco local
resource "databricks_dbfs_file" "init_copy_runner" {
  content_base64 = base64encode(<<SH
#!/bin/bash
set -euo pipefail
echo "[init] Copy runner to /local_disk0" 1>&2
cp /dbfs/FileStore/runner_cli.py /local_disk0/runner_cli.py
chmod 644 /local_disk0/runner_cli.py
SH
  )
  path = "/FileStore/init-copy-runner.sh"
}

# Notebook runner en Workspace para instalar wheel y ejecutar CLI
resource "databricks_notebook" "runner_nb" {
  path            = "/Shared/runner_cli_nb"
  language        = "PYTHON"
  content_base64  = base64encode(<<PYNOTE
# Databricks notebook source
print("[runner-nb] Starting", flush=True)

# Resolve parameters from widgets (or use defaults)
def _get_param(name, default=None):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default

config = _get_param("config", "abfss://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/${var.dataset_config_path}")
env = _get_param("env", "abfss://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/env.yml")
# --- FIX: ensure db_config is defined from widgets before use ---
db_config = _get_param("db_config", "abfss://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/database.yml")

environment = _get_param("environment", "${var.environment_name}")

# Ensure files are local to avoid Python-level auth issues
from urllib.parse import urlparse
import os, yaml

def _copy_to_dbfs_local(uri: str) -> str:
    if uri.startswith("abfs://") or uri.startswith("abfss://"):
        parsed = urlparse(uri)
        fname = os.path.basename(parsed.path)
        dbutils.fs.mkdirs("dbfs:/tmp/mvp")
        dst_dbfs = f"dbfs:/tmp/mvp/{fname}"
        print(f"[runner-nb] Copying {uri} -> {dst_dbfs}", flush=True)
        dbutils.fs.cp(uri, dst_dbfs, True)
        # Return local path under FUSE for Python open()
        return f"/dbfs/tmp/mvp/{fname}"
    return uri

config = _copy_to_dbfs_local(config)
env = _copy_to_dbfs_local(env)
db_config_local = _copy_to_dbfs_local(db_config)

# Decide si se debe omitir la capa Gold (DB) en Databricks
skip_db = False
try:
    with open(db_config_local, "r") as f:
        db_cfg = yaml.safe_load(f) or {}
    # Obtener config del entorno solicitado
    env_cfg = (db_cfg.get("environments", {}) or {}).get(environment) or db_cfg.get(environment) or {}
    conn = env_cfg.get("connection", env_cfg)
    host = str(conn.get("host", ""))
    if host in {"postgres", "localhost"}:
        skip_db = True
except Exception as e:
    print(f"[runner-nb] DB config parse warning: {e}. Will proceed.", flush=True)

# Use the attached job library (wheel) instead of installing via pip
try:
    from pipelines.cli import main as cli_main
except Exception as e:
    import traceback
    print(f"[runner-nb] Import error: {e}", flush=True)
    traceback.print_exc()
    raise

import sys
if skip_db:
    sys.argv = ["cli", "--config", config, "--env", env, "--environment", environment]
else:
    sys.argv = ["cli", "--config", config, "--env", env, "--db-config", db_config_local, "--environment", environment]
print(f"[runner-nb] Executing CLI with args: {sys.argv[1:]}", flush=True)
try:
    cli_main()
except Exception as e:
    import traceback
    print(f"[runner-nb] CLI failed: {e}", flush=True)
    traceback.print_exc()
    raise
PYNOTE
  )
}

# Databricks Job que ejecuta el wheel (entrypoint: cli)
resource "databricks_job" "pipeline" {
  name = "${var.prefix}-pipeline"

  task {
    task_key = "run"

    new_cluster {
      num_workers   = var.num_workers
      spark_version = var.spark_version
      node_type_id  = var.node_type_id

      spark_conf = {
        "fs.azure.account.auth.type.${azurerm_storage_account.adls.name}.dfs.core.windows.net"                    = "OAuth"
        "fs.azure.account.oauth.provider.type.${azurerm_storage_account.adls.name}.dfs.core.windows.net"          = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        "fs.azure.account.oauth2.client.id.${azurerm_storage_account.adls.name}.dfs.core.windows.net"             = var.aad_client_id
        "fs.azure.account.oauth2.client.secret.${azurerm_storage_account.adls.name}.dfs.core.windows.net"         = var.aad_client_secret
        "fs.azure.account.oauth2.client.endpoint.${azurerm_storage_account.adls.name}.dfs.core.windows.net"       = "https://login.microsoftonline.com/${var.tenant_id}/oauth2/token"

        "spark.hadoop.fs.azure.account.auth.type.${azurerm_storage_account.adls.name}.dfs.core.windows.net"       = "OAuth"
        "spark.hadoop.fs.azure.account.oauth.provider.type.${azurerm_storage_account.adls.name}.dfs.core.windows.net" = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        "spark.hadoop.fs.azure.account.oauth2.client.id.${azurerm_storage_account.adls.name}.dfs.core.windows.net"    = var.aad_client_id
        "spark.hadoop.fs.azure.account.oauth2.client.secret.${azurerm_storage_account.adls.name}.dfs.core.windows.net"= var.aad_client_secret
        "spark.hadoop.fs.azure.account.oauth2.client.endpoint.${azurerm_storage_account.adls.name}.dfs.core.windows.net"= "https://login.microsoftonline.com/${var.tenant_id}/oauth2/token"

        // Extra: aplicar OAuth también para stmvpconfigdls01 si se usa en datasets
        "fs.azure.account.auth.type.stmvpconfigdls01.dfs.core.windows.net"                    = "OAuth"
        "fs.azure.account.oauth.provider.type.stmvpconfigdls01.dfs.core.windows.net"          = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        "fs.azure.account.oauth2.client.id.stmvpconfigdls01.dfs.core.windows.net"             = var.aad_client_id
        "fs.azure.account.oauth2.client.secret.stmvpconfigdls01.dfs.core.windows.net"         = var.aad_client_secret
        "fs.azure.account.oauth2.client.endpoint.stmvpconfigdls01.dfs.core.windows.net"       = "https://login.microsoftonline.com/${var.tenant_id}/oauth2/token"

        "spark.hadoop.fs.azure.account.auth.type.stmvpconfigdls01.dfs.core.windows.net"       = "OAuth"
        "spark.hadoop.fs.azure.account.oauth.provider.type.stmvpconfigdls01.dfs.core.windows.net" = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        "spark.hadoop.fs.azure.account.oauth2.client.id.stmvpconfigdls01.dfs.core.windows.net"    = var.aad_client_id
        "spark.hadoop.fs.azure.account.oauth2.client.secret.stmvpconfigdls01.dfs.core.windows.net"= var.aad_client_secret
        "spark.hadoop.fs.azure.account.oauth2.client.endpoint.stmvpconfigdls01.dfs.core.windows.net"= "https://login.microsoftonline.com/${var.tenant_id}/oauth2/token"
        
        "spark.databricks.cluster.profile" = "singleNode"
      }

      spark_env_vars = {
        AZURE_TENANT_ID   = var.tenant_id
        AZURE_CLIENT_ID   = var.aad_client_id
        AZURE_CLIENT_SECRET = var.aad_client_secret
      }
    }

    library {
      whl = databricks_dbfs_file.wheel.dbfs_path
    }

    library {
      pypi {
        package = "adlfs>=2024.4.0"
      }
    }

    library {
      pypi {
        package = "azure-identity>=1.15.0"
      }
    }

    library {
      pypi {
        package = "azure-storage-blob>=12.19.0"
      }
    }

    notebook_task {
      notebook_path = databricks_notebook.runner_nb.path
      base_parameters = {
        config      = "abfss://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/${var.dataset_config_path}"
        env         = "abfss://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/env.yml"
        db_config   = "abfss://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/database.yml"
        environment = var.environment_name
      }
    }
  }
}

# Token de Databricks (para orquestación desde ADF)
resource "databricks_token" "pat" {
  comment          = "ADF orchestration token"
  lifetime_seconds = 86400
}

output "databricks_job_id" {
  value = databricks_job.pipeline.id
}