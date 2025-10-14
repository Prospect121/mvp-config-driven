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
export PIP_CACHE_DIR=/tmp/pip-cache

# Asegurar que los directorios existen y son escribibles
mkdir -p /tmp/pydeps /tmp/pip-cache
# Intentar dar ownership al usuario actual; si falla, abrir permisos
if chown -R "$(id -u)":"$(id -g)" /tmp/pydeps /tmp/pip-cache 2>/dev/null; then
  :
else
  chmod -R 777 /tmp/pydeps /tmp/pip-cache || true
fi

# Instalar dependencias solo si faltan o cambió requirements.txt (cache persistente en volumen)
REQ_FILE=/mvp/requirements.txt
REQ_HASH="$($PYSPARK_PYTHON -c 'import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$REQ_FILE")"
HASH_FILE="/tmp/pydeps/.req_hash"

# Verificar si pandas/pyarrow están disponibles en el entorno actual
DEPS_OK="$($PYSPARK_PYTHON - <<'PY'
import importlib.util
mods = ["pandas", "pyarrow"]
missing = [m for m in mods if importlib.util.find_spec(m) is None]
print("OK" if not missing else "MISS")
PY
)"

SKIP_INSTALL=false
if [ -f "$HASH_FILE" ] && grep -q "$REQ_HASH" "$HASH_FILE"; then
  if [ "$DEPS_OK" = "OK" ]; then
    SKIP_INSTALL=true
    echo "[deps] Hash sin cambios y módulos presentes. Omitiendo pip install."
  else
    echo "[deps] Hash sin cambios pero faltan módulos. Instalando..."
  fi
else
  echo "[deps] Hash cambió o no existe. Instalando..."
fi

if [ "$SKIP_INSTALL" = false ]; then
  set +e
  $PYSPARK_PYTHON -m pip install --disable-pip-version-check --user -r "$REQ_FILE"
  RC=$?
  set -e
  if [ $RC -ne 0 ]; then
    echo "[deps] Falló pip install (rc=$RC). No se actualizará cache de hash."
  else
    echo "$REQ_HASH" > "$HASH_FILE"
  fi
fi

PYMAJORMINOR="$($PYSPARK_PYTHON -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')" 
export PYTHONPATH="/tmp/pydeps/lib/python${PYMAJORMINOR}/site-packages:${PYTHONPATH:-}" 

echo "Using CFG=${CFG}  ENVF=${ENVF}" 
echo "PYSPARK_PYTHON=${PYSPARK_PYTHON}" 
echo "PYTHONPATH=${PYTHONPATH}" 

# ------------- Ejecutar el job ------------- 
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /mvp/jars/postgresql-42.7.2.jar,/mvp/jars/hadoop-aws-3.3.4.jar,/mvp/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.pyspark.python=${PYSPARK_PYTHON} \
  --conf spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON} \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  /mvp/pipelines/spark_job_with_db.py "${CFG}" "${ENVF}" "${DBF}" "${ENV}"
