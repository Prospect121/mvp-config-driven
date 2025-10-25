# MVP Config-Driven Pipeline

Un pipeline de datos modular y dirigido por configuración. Procesa fuentes CSV/JSON/JDBC/API hacia Silver (Parquet) y Gold (Parquet/DB), con transformaciones declarativas y reglas de calidad.

## Inicio Rápido

- Prerrequisitos: Python 3.8+, Java, PySpark, opcional Docker/Compose.
- Entorno:
  - Windows: `python -m venv .venv && .\\.venv\\Scripts\\activate`
  - Linux/macOS: `python -m venv .venv && source .venv/bin/activate`
  - `pip install -r requirements.txt`
- Ejecutar (local):
  - `prodi run-layer raw -c cfg/raw/example.yml`
  - `prodi run-pipeline -p cfg/pipelines/example.yml`
- Docker Compose:
  - `docker compose run --rm runner ./runner.sh --dataset <dataset_name> --env env.yml`

## Ejecución por capa

- Las configuraciones `cfg/<layer>/example.yml` están pensadas para corridas de
  humo. Cada YAML activa `dry_run` y apunta al dataset sintético en
  `samples/toy_customers.csv` para validar la estructura sin tocar servicios
  externos.
- El manifiesto `cfg/pipelines/example.yml` declara la secuencia raw → bronze →
  silver → gold y puede invocarse con `prodi run-pipeline -p ...` desde CI o
  desde cualquier orquestador.

## Notebooks

- `docs/01_pipeline_explicacion.ipynb`: flujo E2E con Quick Start (`USE_QS`) y métricas.
- `docs/01_pipeline_explicacion_min.ipynb`: verificación PySpark/Parquet.

## Estructura del Proyecto (módulos clave)

- `pipelines/utils`: logger estructurado (run_id), concurrencia.
- `pipelines/validation`: reglas de calidad y cuarentena.
- `pipelines/transforms`: transformaciones SQL/UDF y casts.
- `pipelines/config`: carga YAML/JSON.
- `pipelines/io`: lectura/escritura con reintentos y adaptadores multi-nube.
- `pipelines/spark_job_with_db.py`: entrypoint histórico (aún disponible para
  compatibilidad, pero sustituido por `prodi run-layer`).

## Orquestación fuera del código

- `docs/run/databricks.md`, `docs/run/aws.md`, `docs/run/gcp.md` y
  `docs/run/azure.md` contienen guías paso a paso (con JSON/YAML listos) para
  ejecutar el wheel en Databricks, Glue/EMR, Dataproc y Synapse/ADF.
- Los JSON/YAML en `docs/run/jobs/` son importables directamente en los
  orquestadores y referencian las configuraciones `cfg/<layer>/example.yml`.

## Documentación Completa

- Consultar `docs/PROJECT_DOCUMENTATION.md` para arquitectura, configuraciones,
  ejecución detallada, módulos y troubleshooting.

## Consejos

- URIs estándar (`s3://`, `abfss://`, `gs://`): declara la ruta en los YAML y
  define credenciales/opciones en `config/env.yml` o en los `cfg/*.yml` usando
  las claves `storage_options`, `reader_options` y `writer_options`.
- Auditoría: `tools/list_io.py --json` verifica que no haya referencias a
  artefactos en cuarentena ni protocolos no permitidos. `tools/check_cross_layer.py`
  refuerza el aislamiento entre capas y se ejecuta en CI.
- Seguridad: GitGuardian analiza cada PR en GitHub y se complementa con
  `tests/test_security_invariants.py`, que falla si alguien intenta desactivar
  TLS o registrar `AWS_SECRET_ACCESS_KEY`/`fs.s3a.secret.key` en logs.
- Calidad: usa `quarantine` para aislar inválidos sin perderlos.
- Performance: ajustar `spark.sql.shuffle.partitions` y `coalesce/repartition` según volumen.

## Limpieza & Legacy

- Activos cuarentenados se reubican bajo `legacy/` siguiendo la política
  **DEP-001 Legacy Asset Retirement Policy**. Los reportes retirados ahora
  viven en `legacy/docs/2025-10-25-reports/`, los flujos GCP en
  `legacy/infra/2025-10-25-gcp/` y los generadores históricos en
  `legacy/scripts/2025-10-25-generation/`.
- `tools/audit_cleanup.py` genera `docs/cleanup.json` y con `--check`
  impide que reaparezcan archivos marcados como REMOVE o que el core haga
  referencia a rutas cuarentenadas.
- Para revertir temporalmente un activo, muévelo de vuelta a su ruta
  original, notifica al owner indicado en el README local dentro de la
  carpeta `legacy/` correspondiente y vuelve a ejecutar `tools/audit_cleanup.py`
  para actualizar el registro.
