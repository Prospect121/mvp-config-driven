# MVP Config-Driven Pipeline

Un pipeline de datos modular y dirigido por configuración. Procesa fuentes CSV/JSON/JDBC/API hacia Silver (Parquet) y Gold (Parquet/DB), con transformaciones declarativas y reglas de calidad.

## Inicio Rápido

- Prerrequisitos: Python 3.8+, Java, PySpark, opcional Docker/Compose.
- Entorno:
  - Windows: `python -m venv .venv && .\\.venv\\Scripts\\activate`
  - Linux/macOS: `python -m venv .venv && source .venv/bin/activate`
  - `pip install -r requirements.txt`
- Ejecutar (local):
  - `python pipelines/spark_job_with_db.py <dataset_config> config/env.yml config/database.yml development`
- Docker Compose:
  - `docker compose run --rm runner ./runner.sh --dataset <dataset_name> --env env.yml`

## Notebooks

- `docs/01_pipeline_explicacion.ipynb`: flujo E2E con Quick Start (`USE_QS`) y métricas.
- `docs/01_pipeline_explicacion_min.ipynb`: verificación PySpark/Parquet.

## Estructura del Proyecto (módulos clave)

- `pipelines/utils`: logger estructurado (run_id), concurrencia.
- `pipelines/validation`: reglas de calidad y cuarentena.
- `pipelines/transforms`: transformaciones SQL/UDF y casts.
- `pipelines/config`: carga YAML/JSON.
- `pipelines/io`: lectura/escritura con reintentos y adaptadores multi-nube.
- `pipelines/spark_job_with_db.py`: entrypoint principal.

## Documentación Completa

- Consultar `docs/PROJECT_DOCUMENTATION.md` para arquitectura, configuraciones, ejecución detallada, módulos y troubleshooting.

## Consejos

- URIs estándar (`s3://`, `abfss://`, `gs://`): declara la ruta en los YAML y
  define credenciales/opciones en `config/env.yml` o en los `cfg/*.yml` usando
  las claves `storage_options`, `reader_options` y `writer_options`.
- Auditoría: `docs/tools/list_io.py` lista URIs de configuración y marca
  protocolos no soportados.
- Calidad: usa `quarantine` para aislar inválidos sin perderlos.
- Performance: ajustar `spark.sql.shuffle.partitions` y `coalesce/repartition` según volumen.
