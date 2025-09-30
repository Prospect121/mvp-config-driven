#!/usr/bin/env bash
set -euo pipefail

# Carga variables si existen (no falla si falta .env)
if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  source .env || true
fi

# --- Config por defecto e idempotencia ---
NET="${NETWORK_NAME:-mvp-config-driven-net}"
WORKDIR="$(pwd)"

# Fecha del "seed" (puedes sobreescribir con: DATE=2025/09/30 ./scripts/seed.sh)
DATE="${DATE:-2025/09/26}"

# Credenciales/endpoint MinIO
S3A_ENDPOINT="${S3A_ENDPOINT:-http://minio:9000}"
MINIO_USER="${MINIO_ROOT_USER:-minio}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minio12345}"

# Validación mínima del endpoint para mc (debe incluir http:// o https://)
if [[ ! "$S3A_ENDPOINT" =~ ^https?:// ]]; then
  echo "ERROR: S3A_ENDPOINT debe incluir esquema, ej: http://minio:9000"
  exit 1
fi

# Asegura carpeta local para credenciales mc (con rutas WSL/Windows con espacios)
mkdir -p "$WORKDIR/.mc"

echo "==> Configurando alias 'local' en mc…"
docker run --rm --network "$NET" \
  -v "$WORKDIR:/mvp" \
  -v "$WORKDIR/.mc:/root/.mc" \
  minio/mc:latest alias set local "$S3A_ENDPOINT" "$MINIO_USER" "$MINIO_PASS" >/dev/null

# Crea buckets de forma idempotente (si existen, no falla)
echo "==> Creando buckets (raw, silver) si no existen…"
docker run --rm --network "$NET" \
  -v "$WORKDIR/.mc:/root/.mc" \
  minio/mc:latest mb -p local/raw || true

docker run --rm --network "$NET" \
  -v "$WORKDIR/.mc:/root/.mc" \
  minio/mc:latest mb -p local/silver || true

# Limpieza opcional del prefijo de la fecha para evitar “basura” en resiembra
# Descomenta si deseas “borrar y volver a cargar” siempre la misma fecha:
# echo "==> Limpiando prefijo raw/payments/$DATE …"
# docker run --rm --network "$NET" \
#   -v "$WORKDIR/.mc:/root/.mc" \
#   minio/mc:latest rm -r --force "local/raw/payments/$DATE" || true

# Copia sample CSV a la ruta por fecha (si existe, mc lo sobrescribe)
SRC_FILE="/mvp/data/raw/payments/sample.csv"
DST_URI="local/raw/payments/$DATE/sample.csv"
echo "==> Copiando $SRC_FILE -> $DST_URI"
docker run --rm --network "$NET" \
  -v "$WORKDIR:/mvp" \
  -v "$WORKDIR/.mc:/root/.mc" \
  minio/mc:latest cp "$SRC_FILE" "$DST_URI"

echo "==> Listado rápido:"
docker run --rm --network "$NET" \
  -v "$WORKDIR/.mc:/root/.mc" \
  minio/mc:latest ls -r "local/raw/payments/$DATE" || true

echo "Seed OK (DATE=$DATE, ENDPOINT=$S3A_ENDPOINT)"
