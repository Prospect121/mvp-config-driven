# Crea alias, buckets y sube el CSV de ejemplo a MinIO
$ErrorActionPreference = 'Stop'

$WORKDIR = (Get-Location).Path
$ALIAS   = 'local'
$ENDPOINT = 'http://minio:9000'
$AK      = 'minio'
$SK      = 'minio12345'
$NET     = 'mvp-config-driven-net'

# Carpeta para persistir la config de mc
New-Item -ItemType Directory -Force -Path "$WORKDIR\.mc" | Out-Null

# 1) Alias persistente
docker run --rm --network $NET -v "${WORKDIR}:/mvp" -v "${WORKDIR}\.mc:/root/.mc" minio/mc:latest `
  alias set $ALIAS $ENDPOINT $AK $SK

# 2) Buckets (idempotentes)
docker run --rm --network $NET -v "${WORKDIR}:/mvp" -v "${WORKDIR}\.mc:/root/.mc" minio/mc:latest mb -p $ALIAS/raw 2>$null
docker run --rm --network $NET -v "${WORKDIR}:/mvp" -v "${WORKDIR}\.mc:/root/.mc" minio/mc:latest mb -p $ALIAS/silver 2>$null

# 3) Subir CSV
$SRC = "/mvp/data/raw/payments/sample.csv"
$DST = "$ALIAS/raw/payments/2025/09/26/sample.csv"
docker run --rm --network $NET -v "${WORKDIR}:/mvp" -v "${WORKDIR}\.mc:/root/.mc" minio/mc:latest cp "$SRC" "$DST"

Write-Host "Seed OK -> $DST"
