# MVP Config-Driven Pipeline

Plataforma modular de ingesta y transformación basada en configuración. Expone una CLI única (`prodi`) para ejecutar capas Raw → Bronze → Silver → Gold sin dependencias de orquestadores monolíticos.

## Inicio rápido

1. **Requisitos**
   - Python 3.10+ (probado en 3.10–3.12)
   - Java y PySpark disponibles en el `PATH`
2. **Entorno**
   - Windows: `python -m venv .venv && .\.venv\Scripts\activate`
   - Linux/macOS: `python -m venv .venv && source .venv/bin/activate`
   - `pip install -r requirements.txt`
3. **Prueba de humo local**
   - `prodi run-layer raw -c cfg/raw/example.yml`
   - Repetir con `bronze`, `silver` y `gold` para validar el flujo completo usando los datasets sintéticos de `samples/`.
   - El ejemplo de Raw escribe `parquet` en `data/raw/toy_customers/`; las capas superiores permanecen en `dry_run` para evitar side-effects.

## Política de no-Docker

El repositorio ya no contiene archivos de build Docker ni manifests docker compose. CI falla si reaparecen artefactos bajo `docker/`, `.docker/` o manifests de build/compose. Las ejecuciones deben empaquetarse como wheel (`python -m build`) y desplegarse en la plataforma de elección.

## Ejecución por capa con CLI

- Las configuraciones `cfg/<layer>/example.yml` apuntan al dataset de humo `samples/toy_customers.csv`. Raw ya ejecuta la ingesta real (puede cambiarse a Spark ajustando `compute.kind`), mientras que Bronze/Silver/Gold mantienen `dry_run` por defecto.
- `prodi run-layer <layer>` valida esquema, reglas de calidad y escritura de cada capa de forma aislada.
- El pipeline declarativo `cfg/pipelines/example.yml` encadena los mismos YAML y puede ejecutarse con `prodi run-pipeline -p cfg/pipelines/example.yml`.
- Para orquestación externa, ver `docs/run/` (Databricks, AWS Glue/EMR, Dataproc, Azure Synapse/ADF). Los manifiestos en `docs/run/jobs/` muestran cómo invocar el wheel con argumentos `prodi run-layer`.

## Documentación clave

- `docs/PROJECT_DOCUMENTATION.md`: arquitectura, contratos y troubleshooting.
- `docs/STABILIZATION_REPORT.md`: estado de salud y guardas vigentes.
- Notebooks en `docs/` para exploración rápida del modelo de capas.

## Módulos relevantes

- `src/datacore/cli.py`: comandos `prodi run-layer` y utilidades de orquestación.
- `src/datacore/io/*`: adaptadores y lecturas multi-protocolo (fsspec/pandas/polars/spark).
- `src/datacore/quality/*`: reglas y cuarentena declarativa.
- `src/datacore/catalog/*`: normalización de metadatos entre capas.
- `tools/list_io.py`, `tools/check_cross_layer.py`: verificaciones automáticas de I/O y aislamiento.

## Seguridad y cumplimiento

- `tests/test_security_invariants.py` bloquea intentos de deshabilitar TLS o registrar secretos.
- `tools/audit_cleanup.py --check` falla si aparecen rutas prohibidas (legacy, pipelines heredados, Docker) o referencias a cuarentenas eliminadas.
- `tools/list_io.py --json` genera un inventario reproducible de accesos a almacenamiento.

## Limpieza continua

No existe carpeta `legacy`; cualquier activo obsoleto se elimina definitivamente. Los pasos de CI (`ci/lint.yml`, `ci/build-test.yml`) ejecutan `tools/audit_cleanup.py --check` y validaciones adicionales para asegurar que la base permanezca libre de monolitos y artefactos Docker.
