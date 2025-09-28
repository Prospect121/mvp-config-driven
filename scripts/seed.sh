#!/usr/bin/env bash
set -euo pipefail
source .env || true

NET="${NETWORK_NAME:-mvp-config-driven-net}"
WORKDIR="$(pwd)"

mkdir -p "$WORKDIR/.mc"

docker run --rm --network "$NET" -v "$WORKDIR:/mvp" -v "$WORKDIR/.mc:/root/.mc" \
  minio/mc:latest alias set local "${S3A_ENDPOINT:-http://minio:9000}" "${MINIO_ROOT_USER:-minio}" "${MINIO_ROOT_PASSWORD:-minio12345}"

docker run --rm --network "$NET" -v "$WORKDIR:/mvp" -v "$WORKDIR/.mc:/root/.mc" minio/mc:latest mb -p local/raw
docker run --rm --network "$NET" -v "$WORKDIR:/mvp" -v "$WORKDIR/.mc:/root/.mc" minio/mc:latest mb -p local/silver
docker run --rm --network "$NET" -v "$WORKDIR:/mvp" -v "$WORKDIR/.mc:/root/.mc" \
  minio/mc:latest cp /mvp/data/raw/payments/sample.csv local/raw/payments/2025/09/26/sample.csv

echo "Seed OK"
