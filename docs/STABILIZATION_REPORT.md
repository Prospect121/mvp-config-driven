# Stabilization Report – Portable Pipeline Guardrails

## Resumen

- Se reforzó `tools/audit_cleanup.py` para bloquear referencias activas a `/legacy` en código y se añadió un canario de cuarentena
  (`tests/test_cleanup_canary.py`) que demuestra la falla controlada cuando se forzan dichos accesos.
- Se incorporó `tools/check_cross_layer.py` junto con pruebas y jobs de CI para impedir imports cruzados entre raw ↔ bronze ↔
  silver ↔ gold.
- Se añadieron configuraciones declarativas (`cfg/<layer>/example.yml`) y documentación de humo
  multi-nube actualizada (Databricks, Glue/EMR, Dataproc, Synapse/ADF), incluyendo artefactos JSON/YAML listos para importar.
- Se cableó la capa Raw a la implementación activa (`src/datacore/layers/raw/main.py`), leyendo desde `config/datasets/examples/toy_customers.yml`,
  ejecutando transformaciones/dq opcionales y escribiendo `parquet` local para los smokes.
- Se eliminó definitivamente el monolito pipelines legacy y los artefactos Docker; CI ahora falla si reaparecen rutas prohibidas o archivos de build compose.
- Se endurecieron las garantías de seguridad: guardas de TLS siempre activo, escaneo de logging de secretos y documentación sobre
  GitGuardian.

## Evidencias ejecutadas

| Comando / prueba | Resultado |
| --- | --- |
| `python tools/audit_cleanup.py --check` | ✅ Limpieza sin referencias prohibidas |
| `python tools/check_cross_layer.py` | ✅ Sin imports cruzados |
| `LEGACY_CANARY=1 pytest tests/test_cleanup_canary.py` | ✅ Canario falla el audit ante referencias a legacy |
| `pytest` | ✅ 31 pruebas pasadas, 1 omitida (`5cc807†L1-L2`) |
| `bash scripts/smoke_layers.sh` | ✅ Ejecuta Raw (ingesta real) y el resto de capas/pipeline en modo rápido |

## Cambios clave en CI

- `ci/lint.yml` instala `ripgrep`, ejecuta el canario (`LEGACY_CANARY=1 pytest tests/test_cleanup_canary.py`), refuerza el guard de
  capas (`python tools/check_cross_layer.py`) y rechaza cualquier `fs.s3a.connection.ssl.enabled=false` en `src/` o `scripts/`.
- Los pipelines de validación ya existentes (`tools/audit_cleanup.py --check` y `tools/list_io.py --json`) permanecen activos.
- Se añadió el job `smoke` en `ci/build-test.yml`, que instala el proyecto editable, ejecuta `scripts/smoke_layers.sh` y publica `smoke.log` como artefacto.

## Objetivo vs Estado

| Objetivo | Estado | Evidencia |
| --- | --- | --- |
| **Enforcement anti-regresiones**: bloquear `/legacy` y artefactos REMOVE | ✅ `tools/audit_cleanup.py` detecta referencias a
    `legacy` fuera de docs y el canario las reproduce bajo `LEGACY_CANARY=1`. | `tests/test_cleanup_canary.py`, `tools/audit_cleanup.py` |
| **Cero cross-layer**: aislamiento de capas | ✅ Script AST y prueba `tests/test_no_cross_layer.py`; guardas añadidos a CI. |
| **CLI por capa estable** | ✅ `prodi run-layer` se ejerce con `cfg/<layer>/example.yml`; Raw escribe parquet en `data/raw/toy_customers/` y las demás capas permanecen en `dry_run`. | `tests/test_cli_layers.py`, `scripts/smoke_layers.sh` |
| **Portabilidad I/O** | ✅ Configuraciones de ejemplo usan URIs `s3://`, `abfss://`, `gs://`; nuevos docs y jobs (Databricks,
  Glue/EMR, Dataproc, Synapse/ADF) los reutilizan; sin flags inseguros. | `cfg/*/example.yml`, `config/datasets/examples/toy_customers.yml`,
  `docs/run/*` |
| **Smokes multi-nube** | ✅ Documentación actualizada con ejemplos ejecutables y JSON/YAML (`docs/run/jobs/*`). |
| **Security hardening** | ✅ `tests/test_security_invariants.py` vigila TLS/secrets, `ci/lint.yml` usa `rg` para detectar
  overrides, GitGuardian documentado en README/Proyecto. |

## Riesgos y mitigaciones

- **Dependencia de wheel name**: los ejemplos asumen `mvp_config_driven-0.1.0`. Mitigación: documentar que se debe ajustar al
  número de versión publicado.
- **Ambientes sin `ripgrep`**: CI instala la dependencia explícitamente; en entornos locales puede replicarse ejecutando
  `sudo apt-get install ripgrep` o usando el script `tests/test_security_invariants.py` como guardia.
- **Ejecuciones reales**: Raw ejecuta ingesta local por defecto (dataset `samples/`), mientras Bronze/Silver/Gold siguen en `dry_run`. Documentar la transición evita sorpresas al promover configuraciones.

## Observaciones adicionales

- `samples/toy_customers.csv` y su README documentan la generación del dataset de humo.
- GitGuardian continúa activo en GitHub; la documentación explica cómo revisar el check de secretos.
- El pipeline puede etiquetarse como `v0.2.0` una vez que estos guardas se encuentren en la rama base `feature/main-codex`.
