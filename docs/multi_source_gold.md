# Ingesta Multi-fuente y Gold en Bucket/DB

Este cambio habilita:
- Ingesta desde múltiples fuentes en un solo pipeline (`sources`): buckets/archivos, APIs (con paginación y staging opcional), y bases de datos vía JDBC.
- Capa Gold configurable para escribir en base de datos (con mapeo de esquema y upsert) y/o en bucket (S3/MinIO) con particionado, compresión y coalesce.
- Compatibilidad retro: `source` único sigue funcionando; Gold a DB existente sigue intacto.

## Cómo usar

- Ejecutar:
  `python pipelines/spark_job_with_db.py config/datasets/finanzas/payments_multi/dataset.yml config/env.yml config/database.yml default`

- Dataset de ejemplo:
  `config/datasets/finanzas/payments_multi/dataset.yml` define tres fuentes: JDBC, API y archivo (jsonl en bucket). La salida Silver y Gold están en formato `parquet` con particionado por `year, month` derivado de `created_at`.

## Configuración de múltiples fuentes

- Clave `sources:` acepta una lista donde cada entrada puede ser:
  - `type: jdbc` con `jdbc.url`, `jdbc.table`, `jdbc.user`, `jdbc.password`, `jdbc.driver`.
  - `type: api` con `api.method`, `api.endpoint`, `api.headers_ref`, `page_*` y `staging` opcional para persistir RAW.
  - `type: file` con `path` y `input_format` (`csv`, `json`, `jsonl`, `parquet`).
- El loader une todos los DataFrames mediante `unionByName`, añadiendo columnas faltantes con `null` para compatibilidad de esquema.

## Gold: base de datos y bucket

- Base de datos:
  - Usa `output.gold.enabled` o `output.gold.database.enabled`.
  - Mapea esquema desde `schema.ref` (JSON Schema), crea/actualiza tabla y escribe con `default_write_mode` y `upsert_keys` de `database.yml` o overrides del dataset (`write_mode`, `upsert_keys`, `table_prefix/suffix`).
- Bucket:
  - Configurar `output.gold.bucket.enabled: true`, `path`, `format`, `mode`, `compression`, `merge_schema`, `partition_by`, `partition_from` y `coalesce`/`repartition`.
  - Se derivan columnas de partición (`year`, `month`, `day`, `date`) desde `partition_from` si faltan.

## Calidad y robustez

- Validaciones (quarantine/fail/drop/warn) se aplican antes de Silver.
- Manejo de errores: el pipeline registra inicio y estado (`completed/failed`) en la base de datos si está habilitada; cierre seguro de Spark en éxito o error.
- Compatibilidad garantizada: si `sources` no se define, se usa `source` único; si Gold no está configurado, el pipeline completa tras Silver.

## Notas

- `headers_ref` mapea encabezados desde `env.yml` usando prefijos (ej.: `API_HEADERS_PAYMENTS_*`).
- Para buckets S3/MinIO se usa auto-configuración con `maybe_config_s3a`.