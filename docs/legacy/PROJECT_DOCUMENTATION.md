# Documentación Técnica del MVP Config-Driven

Este documento resume la arquitectura vigente tras la eliminación del monolito pipelines legacy y el retiro definitivo de artefactos Docker. Todo el flujo se opera mediante la CLI `prodi` y configuraciones declarativas por capa.

## Arquitectura por capas

- **Capas**: Raw → Bronze → Silver → Gold. Cada capa ejecuta validaciones y escrituras independientes.
- **Contratos**: YAML bajo `cfg/<layer>/` describen entradas, transformaciones y salidas. `config/env.yml` aporta parámetros comunes (Spark, credenciales) y `config/database.yml` define destinos Gold opcionales.
- **CLI**: `src/datacore/cli.py` expone `prodi run-layer <layer> -c <cfg>` para ejecuciones locales o orquestadas. No existe `prodi run-pipeline` como punto de partida recomendado.

## Componentes principales

- `src/datacore/io/`: lectura/escritura multiformato con `fsspec`, pandas/polars y Spark.
- `src/datacore/quality/`: reglas, cuarentena declarativa y métricas agregadas.
- `src/datacore/catalog/`: normalización de metadatos y contratos entre capas.
- `src/datacore/runtime/`: utilidades de orquestación, logging estructurado y manejo de `run_id`.
- `tools/check_cross_layer.py`: evita imports cruzados entre capas.
- `tools/list_io.py`: inventario de llamadas I/O y detección de runners heredados.
- `tools/audit_cleanup.py`: valida que no existan rutas prohibidas ni referencias a activos retirados.

## Ejecución local

1. Crear y activar entorno virtual.
2. Instalar dependencias (`pip install -r requirements.txt`).
3. Ejecutar pruebas de humo:
   ```bash
   prodi run-layer raw -c cfg/raw/example.yml
   prodi run-layer bronze -c cfg/bronze/example.yml
   prodi run-layer silver -c cfg/silver/example.yml
   prodi run-layer gold -c cfg/gold/example.yml
   ```
   Raw genera archivos `parquet` en `data/raw/toy_customers/` usando el dataset de muestra, mientras que Bronze/Silver/Gold permanecen en `dry_run` para acelerar validaciones.
   Para ejecutar todas las capas declarativamente usa `prodi run-pipeline -p cfg/pipelines/example.yml`.

## Orquestación externa

- `docs/run/databricks.md`: ejecutar el wheel en Databricks Jobs.
- `docs/run/aws.md`: AWS Glue/EMR y Step Functions invocando `prodi run-layer`.
- `docs/run/gcp.md`: Dataproc y Composer con los mismos comandos.
- `docs/run/azure.md`: Synapse/ADF usando actividades de Spark con argumentos CLI.
- `docs/run/jobs/*.json|yaml`: manifiestos listos para importar que pasan los parámetros `run-layer` adecuados.

## Seguridad y cumplimiento

- `tests/test_security_invariants.py` impide desactivar TLS (`fs.s3a.connection.ssl.enabled=false`) o loguear secretos.
- `tests/test_cleanup_canary.py` confirma que `tools/audit_cleanup.py --check` falla ante nuevas referencias a `legacy`.
- CI (`ci/lint.yml`, `ci/build-test.yml`) ejecuta:
  - `python tools/audit_cleanup.py --check`
  - `python tools/list_io.py --json`
  - `python tools/check_cross_layer.py`
  - `pytest`
  para asegurar cumplimiento continuo.

## Pruebas y estilo

- `pytest`: suites en `tests/` cubren CLI, invariantes de seguridad y aislamiento de capas.
- `black`, `ruff`, `mypy`, `pre-commit`: definidos en `pyproject.toml`.
- `tests/test_cli_layers.py` ejecuta la ingesta Raw extremo a extremo (escritura de parquet) y verifica que el resto de capas continúen aceptando `dry_run`.

## Mantenimiento y limpieza

- No existe carpeta `legacy`. Los activos obsoletos se eliminan; si reaparecen, `tools/audit_cleanup.py --check` falla.
- El script también bloquea cualquier archivo de build Docker, manifests compose o carpetas `docker/`/`.docker/`.
- `docs/cleanup.json` y `docs/CLEANUP_REPORT.md` se actualizan para reflejar el inventario vigente sin referencias a pipelines.

Para una introducción rápida, revisa `README.md`. Para detalles operativos específicos por plataforma, consulta `docs/run/`.
