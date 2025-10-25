#!/usr/bin/env bash
set -euo pipefail

# ------------- Config (rutas por defecto) -------------
CFG=${CFG:-"/mvp/config/datasets/finanzas/payments_v1/dataset.yml"}
ENVF=${ENVF:-"/mvp/config/env.yml"}
DBF=${DBF:-"/mvp/config/database.yml"}
ENV=${ENV:-"development"}

# ------------- Environment variables provided by secure credential store -------------
: "${DB_HOST:?DB_HOST must be provided by a secure credential store}"
: "${DB_PORT:?DB_PORT must be provided by a secure credential store}"
: "${DB_NAME:?DB_NAME must be provided by a secure credential store}"
: "${DB_USER:?DB_USER must be provided by a secure credential store}"
: "${DB_PASSWORD:?DB_PASSWORD must be provided by a secure credential store}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID must be provided by a secure credential store}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY must be provided by a secure credential store}"
AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_REGION
export DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
S3A_ENDPOINT="${S3A_ENDPOINT_URL:-${AWS_ENDPOINT_URL:-}}"

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
echo "Environment variables validated via secure provider."

# ------------- Ejecutar el job con base de datos -------------
SPARK_SUBMIT_ARGS=(
  --master spark://spark-master:7077
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262
  --conf spark.pyspark.python=${PYSPARK_PYTHON}
  --conf spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON}
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
  --conf spark.hadoop.fs.s3a.path.style.access=true
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider
)

if [ -n "${S3A_ENDPOINT}" ]; then
  SPARK_SUBMIT_ARGS+=(--conf spark.hadoop.fs.s3a.endpoint=${S3A_ENDPOINT})
fi

/opt/bitnami/spark/bin/spark-submit \
  "${SPARK_SUBMIT_ARGS[@]}" \
  /mvp/pipelines/spark_job_with_db.py "${CFG}" "${ENVF}" "${DBF}" "${ENV}"

echo "Pipeline execution completed."
