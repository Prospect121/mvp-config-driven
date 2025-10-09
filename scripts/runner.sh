#!/usr/bin/env bash 
set -euo pipefail 

# ------------- Config (rutas por defecto) ------------- 
CFG=${CFG:-"/mvp/config/datasets/finanzas/payments_v1/dataset.yml"} 
ENVF=${ENVF:-"/mvp/config/env.yml"}
DBF=${DBF:-"/mvp/config/database.yml"}
ENV=${ENV:-"development"} 

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

echo "Using CFG=${CFG}  ENVF=${ENVF}" 
echo "PYSPARK_PYTHON=${PYSPARK_PYTHON}" 
echo "PYTHONPATH=${PYTHONPATH}" 

# ------------- Ejecutar el job ------------- 
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /mvp/postgresql-42.7.2.jar,/mvp/jars/hadoop-aws-3.3.4.jar,/mvp/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.pyspark.python=${PYSPARK_PYTHON} \
  --conf spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON} \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  /mvp/pipelines/spark_job_with_db.py "${CFG}" "${ENVF}" "${DBF}" "${ENV}"