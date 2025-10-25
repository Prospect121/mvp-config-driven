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

## Validación de configuraciones y estado incremental

- `prodi validate -c cfg/finance/raw/transactions_http.yml` ejecuta únicamente la validación de esquema sobre un YAML.
- Todos los comandos aceptan `--dq-fail-on-error/--dq-no-fail-on-error` para sobreescribir la severidad efectiva de los checks.
- Los watermarks incrementales se persisten automáticamente en `data/_state/<state_id>.json`; basta con declarar `incremental.watermark.state_id` en cada fuente para reutilizar el último valor exitoso.

### Autenticación soportada

| Bloque (`io.source`) | Tipo (`auth.type`)         | Uso previsto                                                         |
| -------------------- | -------------------------- | -------------------------------------------------------------------- |
| HTTP                 | `bearer_env`               | Inserta `Authorization: Bearer <token>` leyendo la variable `env`.    |
|                      | `api_key_header`           | Configura el header indicado con el valor tomado del entorno.        |
|                      | `basic_env`                | Resuelve `username_env`/`password_env` sin escribir secretos en YAML. |
|                      | `oauth2_client_credentials`| Ejecuta el flujo client-credentials (`token_url`, `scope`, timeout).  |
| JDBC                 | `managed_identity`         | Enciende `azure.identity.auth.type=ManagedIdentity`.                  |
|                      | `basic_env`                | Usa credenciales almacenadas en variables de entorno.                |

## Política de no-Docker

El repositorio ya no contiene archivos de build Docker ni manifests docker compose. CI falla si reaparecen artefactos bajo `docker/`, `.docker/` o manifests de build/compose. Las ejecuciones deben empaquetarse como wheel (`python -m build`) y desplegarse en la plataforma de elección.

## Credenciales por identidad

- **AWS**: los YAML `cfg/*/aws.prod.yml` asumen que los jobs corren con un rol IAM asociado a la instancia/servicio. El archivo `config/env.aws.prod.yml` habilita TLS (`use_ssl: true`) y firma SigV4 sin exponer llaves.
- **Azure**: las variantes `cfg/*/azure.prod.yml` se autentican mediante Managed Identity; `config/env.azure.prod.yml` sólo declara `auth: managed_identity` y opciones TLS.
- **GCP**: las configuraciones `cfg/*/gcp.prod.yml` dependen de cuentas de servicio sin llaves gracias a Workload Identity (`config/env.gcp.prod.yml`).

## Ejecución por capa con CLI

- Las configuraciones `cfg/<layer>/example.yml` apuntan al dataset de humo `samples/toy_customers.csv`. Raw ya ejecuta la ingesta real (puede cambiarse a Spark ajustando `compute.kind`), mientras que Bronze/Silver/Gold mantienen `dry_run` por defecto.
- `prodi run-layer <layer>` valida esquema, reglas de calidad y escritura de cada capa de forma aislada.
- El pipeline declarativo `cfg/pipelines/example.yml` encadena los mismos YAML y puede ejecutarse con `prodi run-pipeline -p cfg/pipelines/example.yml`.
- Para orquestación externa, ver `docs/run/` (Databricks, AWS Glue/EMR, Dataproc, Azure Synapse/ADF). Los manifiestos en `docs/run/jobs/` muestran cómo invocar el wheel con argumentos `prodi run-layer`.
- Forzar `PRODI_FORCE_DRY_RUN=1` permite validar configuraciones productivas (`cfg/*/*.prod.yml`) sin ejecutar escrituras reales; este mismo override se usa en CI (`smoke-prod`).

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
