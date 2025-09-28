#!/usr/bin/env bash
set -euo pipefail

# ------------- Config (rutas por defecto) -------------
CFG=${CFG:-"/mvp/config/datasets/finanzas/payments_v1/dataset.yml"}
ENVF=${ENVF:-"/mvp/config/env.yml"}

# ------------- Python dentro de la imagen Bitnami -------------
# Usamos el intérprete “oficial” que trae la imagen
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
  --conf spark.pyspark.python=${PYSPARK_PYTHON} \
  --conf spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON} \
  /mvp/pipelines/spark_job.py "${CFG}" "${ENVF}"
