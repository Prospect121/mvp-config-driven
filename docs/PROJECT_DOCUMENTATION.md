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
- `pipelines/spark_job_with_db.py`: entrypoint histórico con integración DB. Se
  mantiene para compatibilidad, pero la ruta recomendada es el CLI `prodi`.
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

### Preparación local

- Crear entorno: `python -m venv .venv` y activar (`source .venv/bin/activate` en Linux/macOS o `.\.venv\Scripts\activate` en
  Windows).
- Instalar dependencias: `pip install -r requirements.txt`.

### Cómo correr por capa (CLI)

- `prodi run-layer <layer> -c cfg/<layer>/example.yml` ejecuta validaciones en modo `dry_run`. Los YAML usan el dataset sintético
  `samples/toy_customers.csv` y rutas multi-nube (`s3://`, `abfss://`, `gs://`).
- `prodi run-pipeline -p cfg/pipelines/example.yml` recorre la cadena completa (raw → bronze → silver → gold) reutilizando los
  mismos YAML.
- Los tests `tests/test_cli_layers.py` y `tests/test_cli_smoke.py` aseguran que cada capa se mantenga independiente y que el
  pipeline declarativo no encadene ejecuciones implícitas.

### Docker Compose (opcional)

- `docker compose run --rm runner ./runner.sh --dataset <dataset_name> --env env.yml` mantiene el flujo heredado para entornos de
  laboratorio. Los nuevos comandos Typer están disponibles dentro del contenedor (`prodi run-layer ...`).

### Cómo orquestar fuera del código

- `docs/run/databricks.md`: wheel tasks de Databricks Jobs + JSON [`docs/run/jobs/databricks_job.json`].
- `docs/run/aws.md`: jobs de Glue/Step Functions y pasos EMR (`docs/run/jobs/aws_stepfunctions.json`, `docs/run/jobs/emr_steps.json`).
- `docs/run/gcp.md`: workflows de Dataproc con manifiesto [`docs/run/jobs/dataproc_workflow_v2.yaml`].
- `docs/run/azure.md`: Spark job definitions y plantilla de Data Factory (`docs/run/jobs/azure_adf_pipeline.json`).
- Todos los ejemplos consumen `cfg/<layer>/example.yml` y respetan TLS habilitado por defecto.

### Notebooks

- `docs/01_pipeline_explicacion.ipynb`: flujo E2E con Quick Start (`USE_QS`), métricas por etapa y verificación.
- `docs/01_pipeline_explicacion_min.ipynb`: validación rápida de PySpark y Parquet.

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
- Guardas automáticas:
  - `tests/test_security_invariants.py` falla si se detectan `fs.s3a.connection.ssl.enabled=false` o patrones de logging de
    secretos (`AWS_SECRET_ACCESS_KEY`, `fs.s3a.secret.key`).
  - `tests/test_no_cross_layer.py` y `tools/check_cross_layer.py` aseguran el aislamiento raw ↔ bronze ↔ silver ↔ gold.
  - GitGuardian se ejecuta en GitHub y alerta sobre secretos accidentales.

## Troubleshooting

- S3A/MinIO: asegurar JARs (`hadoop-aws`, `aws-java-sdk-bundle`) en `jars/`; credenciales válidas.
- JDBC: confirmar conectividad y credenciales; ajustar `partitionColumn`/`numPartitions` si el volumen lo amerita.
- Calidad: revisar `rules` para evitar filtrar todo; usar `quarantine` para inspección.

## Decisiones y Trade-offs

- Parquet por compatibilidad y performance; Delta opcional a futuro.
- Particionado por fecha balanceando archivos y lectura.
- Modularización para mantenibilidad y pruebas.

---

## Limpieza & Legacy

- La política **DEP-001 Legacy Asset Retirement Policy** exige cuarentena de 30 días con trazabilidad y reversibilidad. Todos los
  activos retirados se almacenan bajo `legacy/` con un `README.md` local que documenta owner, motivo y fecha.
- `legacy/docs/2025-10-25-reports/` mantiene los artefactos de reportes (`REPORT.md`, `report.json`) con instrucciones de
  restauración.
- `legacy/infra/2025-10-25-gcp/` conserva el workflow `dataproc_workflow.yaml` retirado de GCP.
- `legacy/scripts/2025-10-25-generation/` almacena el generador histórico `generate_big_payments.py`.
- Utiliza `tools/audit_cleanup.py` para regenerar `docs/cleanup.json` y `tools/list_io.py --json` para garantizar que el core no
  apunte a estos recursos. Cualquier reversión temporal debe notificarse al owner indicado y quedar registrada.

Para documentación básica y guía rápida, ver `README.md` en la raíz del proyecto.
