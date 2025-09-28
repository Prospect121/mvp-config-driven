#!/bin/bash
set -euo pipefail

# ---------- Identidad & HOME ----------
export HOME="${HOME:-/tmp}"
mkdir -p "$HOME" "$HOME/.local" /tmp/.ivy2/local /tmp/spark-warehouse

# Usuario visible para Spark/Hadoop
export USER="${USER:-spark}"
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-spark}"

# ---------- nss_wrapper: entrada passwd/grupo fake ----------
# Bitnami trae libnss_wrapper (ruta suele estar en LIBNSS_WRAPPER_PATH)
# Creamos archivos passwd/group efímeros para mapear el UID/GID al nombre "spark"
UID_CUR=$(id -u)
GID_CUR=$(id -g)
echo "spark:x:${UID_CUR}:${GID_CUR}:Spark User:/tmp:/sbin/nologin" > /tmp/nss_passwd
echo "sparkgrp:x:${GID_CUR}:" > /tmp/nss_group

# Cargar nss_wrapper (si existe)
export NSS_WRAPPER_PASSWD=/tmp/nss_passwd
export NSS_WRAPPER_GROUP=/tmp/nss_group
export LD_PRELOAD="${LIBNSS_WRAPPER_PATH:-/opt/bitnami/common/lib/libnss_wrapper.so}"

# ---------- pip sin caché y en modo usuario ----------
export PIP_NO_CACHE_DIR=1

# Detecta Python
PYBIN=""
if [ -x /opt/bitnami/python/bin/python3 ]; then
  PYBIN=/opt/bitnami/python/bin/python3
elif command -v python3 >/dev/null 2>&1; then
  PYBIN=$(command -v python3)
else
  mkdir -p /var/lib/apt/lists/partial /var/cache/apt/archives/partial || true
  apt-get update -y
  apt-get install -y --no-install-recommends python3 python3-pip
  rm -rf /var/lib/apt/lists/*
  PYBIN=$(command -v python3)
fi

# Instala deps en ~/.local
"$PYBIN" -m pip install --user -r requirements.txt || true

# PYTHONPATH a site-packages de usuario
PYVER=$("$PYBIN" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
export PYTHONPATH="$HOME/.local/lib/python${PYVER}/site-packages:${PYTHONPATH:-}"

# PySpark usa el mismo intérprete
export PYSPARK_PYTHON="$PYBIN"
export PYSPARK_DRIVER_PYTHON="$PYBIN"

# ---------- spark-submit ----------
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.access.key="${MINIO_ROOT_USER:-minio}" \
  --conf spark.hadoop.fs.s3a.secret.key="${MINIO_ROOT_PASSWORD:-minio12345}" \
  --conf spark.hadoop.security.authentication=simple \
  --conf spark.sql.warehouse.dir=file:///tmp/spark-warehouse \
  pipelines/spark_job.py \
  config/datasets/finanzas/payments_v1/dataset.yml \
  config/env/local.yml
