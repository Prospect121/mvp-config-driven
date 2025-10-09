#!/usr/bin/env bash
set -euo pipefail

# ------------- Config (rutas por defecto) -------------
CFG=${CFG:-"/mvp/config/datasets/finanzas/payments_v1/dataset.yml"}
ENVF=${ENVF:-"/mvp/config/env.yml"}
DBF=${DBF:-"/mvp/config/database.yml"}
ENV=${ENV:-"development"}

# ------------- Environment variables for database connection -------------
export DB_HOST=${DB_HOST:-postgres}
export DB_PORT=${DB_PORT:-5432}
export DB_NAME=${DB_NAME:-data_warehouse}
export DB_USER=${DB_USER:-postgres}
export DB_PASSWORD=${DB_PASSWORD:-postgres123}

# ------------- Environment variables for S3A/MinIO connection -------------
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-minio}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-minio12345}
export AWS_REGION=${AWS_REGION:-us-east-1}

# ------------- Python dentro de la imagen Bitnami -------------
# Usamos el intérprete "oficial" que trae la imagen
export PYSPARK_PYTHON=/opt/bitnami/python/bin/python3
export PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python3

# ------------- Instalar dependencias del repo en /tmp (sin permisos de HOME) -------------
# Instalamos en una base de usuario temporal y añadimos su site-packages a PYTHONPATH
export PYTHONUSERBASE=/tmp/pydeps
$PYSPARK_PYTHON -m pip install --no-cache-dir --disable-pip-version-check --user -r /mvp/requirements.txt || true

PYMAJORMINOR="$($PYSPARK_PYTHON -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
export PYTHONPATH="/tmp/pydeps/lib/python${PYMAJORMINOR}/site-packages:${PYTHONPATH:-}"

echo "Starting Spark pipeline with database integration..."
echo "Using CFG=${CFG}  ENVF=${ENVF}  DBF=${DBF}  ENV=${ENV}"
echo "PYSPARK_PYTHON=${PYSPARK_PYTHON}"
echo "PYTHONPATH=${PYTHONPATH}"
echo "Environment variables set:"
echo "DB_HOST: $DB_HOST"
echo "DB_PORT: $DB_PORT"
echo "DB_NAME: $DB_NAME"
echo "DB_USER: $DB_USER"
echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"

# ------------- Ejecutar el job con base de datos -------------
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.pyspark.python=${PYSPARK_PYTHON} \
  --conf spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON} \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  /mvp/pipelines/spark_job_with_db.py "${CFG}" "${ENVF}" "${DBF}" "${ENV}"

echo "Pipeline execution completed."