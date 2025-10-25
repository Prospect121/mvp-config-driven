# Documentación Técnica y Configuraciones (Proyecto MVP Config-Driven)

Este documento centraliza la arquitectura, módulos, configuraciones, ejecución y mejores prácticas del proyecto. Refleja el estado actual del código y cómo operar los pipelines de datos.

## Arquitectura

- Capas:
  - Source → Silver (Parquet en FS/S3A) → Gold (Parquet/DB).
  - Metadata (PostgreSQL) para ejecuciones y versionado (opcional).
- Patrón: Pipeline dirigido por configuración (`dataset.yml`, `env.yml`, `database.yml`).
- Módulos principales:
  - `pipelines/utils`: `logger.py` (logging con `run_id`), `parallel.py` (concurrencia).
  - `pipelines/validation`: `quality.py` (reglas: quarantine/drop/warn/fail, stats, cuarentena).
  - `pipelines/transforms`: `apply.py` (SQL/UDF con `safe_cast`, `create|replace`, `on_error`).
  - `pipelines/config`: `loader.py` (carga YAML/JSON).
  - `pipelines/io`: `reader.py`/`writer.py` (reintentos `tenacity`, particiones), `s3a.py` (config S3A).
  - `pipelines/spark_job_with_db.py`: entrypoint principal con integración DB (metadata/versionado).
  - `pipelines/sources.py`: ingesta multi-fuente (CSV/JSON/JSONL/Parquet/JDBC/API, union). 

## Configuraciones

- `config/env.yml`:
  - `spark.master`, `timezone`, credenciales S3A (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` si aplica).
- `config/datasets/<...>/dataset.yml`:
  - `source` o `sources` (multi-fuente) con `input_format`, `path`, `options`, o `jdbc`/`api`.
  - `standardization.renames`, `standardization.casts`.
  - `json_normalization.flatten`, `null_handling.fills/drop_if_null`.
  - `transforms_ref` (archivo `transforms.yml`).
  - `quality.quarantine`, `quality.rules` con `filter/expr` y `action/on_fail`.
  - `output.silver` (ruta/particiones) y `output.gold` ({ bucket.enabled/path/exclude_columns, db.enabled }).
- `config/database.yml`:
  - Engine PostgreSQL: host/port/db, credenciales.
  - `table_settings`: `default_write_mode`, `upsert_keys`, etc.

## Ejecución

- Local (Windows/macOS/Linux):
  - `python -m venv .venv && .\.venv\Scripts\activate` (Windows) o `source .venv/bin/activate`.
  - `pip install -r requirements.txt`.
  - `python pipelines/spark_job_with_db.py <dataset_config> config/env.yml config/database.yml development`.
- Docker Compose:
  - `docker compose run --rm runner ./runner.sh --dataset <dataset_name> --env env.yml`.
  - Servicios: `postgres`, `minio`, `spark-master`, `spark-worker-1`, `runner`.
- Notebooks:
  - `docs/01_pipeline_explicacion.ipynb`: flujo E2E con Quick Start (`USE_QS`), métricas por etapa y verificación.
  - `docs/01_pipeline_explicacion_min.ipynb`: validación rápida de PySpark y Parquet.
- Operación en plataformas cloud:
  - `docs/run/databricks.md`: wheel tasks de Databricks Jobs con ejemplos `dry-run` y dependencia por capa.
  - `docs/run/aws.md`: despliegue en AWS Glue + Step Functions.
  - `docs/run/gcp.md`: workflows de Dataproc con templates parametrizados.
  - `docs/run/azure.md`: ejecución en Synapse Spark job definitions.
  - `docs/run/jobs/`: artefactos JSON/YAML listos para importar (Jobs, Workflows, Step Functions).

## Ingesta Multi-Fuente

- `pipelines/sources.py` soporta:
  - Archivos: `csv`, `json`, `jsonl`, `parquet` (con `options` por formato).
  - JDBC: `url`, `table`, `user`, `password`, opciones de particionado (si aplica).
  - API: `method`, `endpoint`, `headers_ref`, paginación (GET/POST), staging opcional.
  - `load_sources_or_source` une múltiples fuentes con columnas alineadas.

## Transformaciones

- Declarativas SQL/UDF:
  - `target_column/name`, `expr/function`, `mode=create|replace`, `type` (cast), `on_error=null|skip`.
- Limpieza y normalización:
  - Renombres previos a validación; `sanitize_nulls` (relleno y filtrado por nulos); `dropDuplicates`.
  - `flatten_json` para estructuras anidadas.

## Calidad de Datos

- Reglas con acciones:
  - `quarantine`: escribe inválidos con razones (`_failed_rules`) y `run_id`.
  - `drop`: descarta; `warn`: solo contabiliza; `fail`: aborta.
- Estadísticas `stats`: `quarantine_count`, `dropped_count`, `warn_count`.

## Escritura Silver/Gold

- `writer.py`: Parquet con `partitionBy` y heurísticas de `coalesce`/`repartition` (opcional).
- Particionado típico: `year/month` derivado de `created_at`.
- Gold (DB): `DatabaseManager` soporta `append` y `UPSERT` (si `upsert_keys`). Registra ejecuciones (`metadata.pipeline_executions`) y versiones.

## Métricas y Logging

- `utils/logger.get_logger(module, run_id)`: logs estructurados por módulo.
- Métricas notebook: tiempos por etapa, conteos por capa, stats de calidad.

## Testing y Estilo

- Pruebas mínimas: `pipelines/transforms/tests/test_apply.py` (PyTest).
- Estilo: `black`, `ruff`, `mypy`, `pre-commit`. Config en `pyproject.toml` y `.pre-commit-config.yaml`.

## Troubleshooting

- S3A/MinIO: asegurar JARs (`hadoop-aws`, `aws-java-sdk-bundle`) en `jars/`; credenciales válidas.
- JDBC: confirmar conectividad y credenciales; ajustar `partitionColumn`/`numPartitions` si el volumen lo amerita.
- Calidad: revisar `rules` para evitar filtrar todo; usar `quarantine` para inspección.

## Decisiones y Trade-offs

- Parquet por compatibilidad y performance; Delta opcional a futuro.
- Particionado por fecha balanceando archivos y lectura.
- Modularización para mantenibilidad y pruebas.

---

Para documentación básica y guía rápida, ver `README.md` en la raíz del proyecto.