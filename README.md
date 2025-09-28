# MVP config-driven (Windows + Docker) — usando /mvp

## Requisitos
- Docker Desktop (WSL2)
- Puertos 9000, 9001, 8080 libres

## Uso
1) Crea `.env` con:
   MINIO_ROOT_USER=minio
   MINIO_ROOT_PASSWORD=minio12345

2) Levanta servicios:
   docker compose up -d --build

3) Semilla (alias + buckets + CSV):
   ./scripts/seed.ps1

4) Ejecuta pipeline:
   ./scripts/run_pipeline.ps1

5) Verifica en MinIO:
   http://localhost:9001 (minio/minio12345)
   Debes ver:
   - raw/payments/2025/09/26/sample.csv
   - silver/payments_v1/... (Parquet)

6) Bajar:
   ./scripts/teardown.ps1


cd "/mnt/c/Users/erick/Documents/work/Prodigio/A - ruta pass/mvp-config-driven"
pwd    # confirma que estás en mvp-config-driven

sudo apt-get update -y && sudo apt-get install -y dos2unix
dos2unix scripts/runner.sh

make up        # levanta minio + spark
make seed      # crea alias/buckets y sube el CSV de ejemplo
make run       # ejecuta el pipeline (runner -> spark-submit)
make logs      # logs si quieres verlos
make down      # bajar todo y limpiar volúmenes