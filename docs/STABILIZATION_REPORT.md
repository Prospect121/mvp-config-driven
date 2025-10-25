# Stabilization Report – Portable Pipeline Guardrails

## Resumen

- Se reforzó `tools/audit_cleanup.py` para bloquear referencias activas a `/legacy` en código y se añadió un canario de cuarentena
  (`tests/test_cleanup_canary.py`) que demuestra la falla controlada cuando se forzan dichos accesos.
- Se incorporó `tools/check_cross_layer.py` junto con pruebas y jobs de CI para impedir imports cruzados entre raw ↔ bronze ↔
  silver ↔ gold.
- Se añadieron configuraciones declarativas (`cfg/<layer>/example.yml`, `cfg/pipelines/example.yml`) y documentación de humo
  multi-nube actualizada (Databricks, Glue/EMR, Dataproc, Synapse/ADF), incluyendo artefactos JSON/YAML listos para importar.
- Se endurecieron las garantías de seguridad: guardas de TLS siempre activo, escaneo de logging de secretos y documentación sobre
  GitGuardian.

## Evidencias ejecutadas

| Comando / prueba | Resultado |
| --- | --- |
| `python tools/audit_cleanup.py --check` | ✅ Limpieza sin referencias prohibidas |
| `python tools/check_cross_layer.py` | ✅ Sin imports cruzados |
| `LEGACY_CANARY=1 pytest tests/test_cleanup_canary.py` | ✅ Canario falla el audit ante referencias a legacy |
| `pytest` | ✅ 31 pruebas pasadas, 1 omitida (`5cc807†L1-L2`) |

## Cambios clave en CI

- `ci/lint.yml` instala `ripgrep`, ejecuta el canario (`LEGACY_CANARY=1 pytest tests/test_cleanup_canary.py`), refuerza el guard de
  capas (`python tools/check_cross_layer.py`) y rechaza cualquier `fs.s3a.connection.ssl.enabled=false` en `src/` o `scripts/`.
- Los pipelines de validación ya existentes (`tools/audit_cleanup.py --check` y `tools/list_io.py --json`) permanecen activos.

## Objetivo vs Estado

| Objetivo | Estado | Evidencia |
| --- | --- | --- |
| **Enforcement anti-regresiones**: bloquear `/legacy` y artefactos REMOVE | ✅ `tools/audit_cleanup.py` detecta referencias a
  `legacy/` fuera de docs y el canario las reproduce bajo `LEGACY_CANARY=1`. | `tests/test_cleanup_canary.py`, `tools/audit_cleanup.py` |
| **Cero cross-layer**: aislamiento de capas | ✅ Script AST y prueba `tests/test_no_cross_layer.py`; guardas añadidos a CI. |
| **CLI por capa estable** | ✅ `prodi run-layer` y `prodi run-pipeline` se ejercen con `cfg/<layer>/example.yml` y
  `cfg/pipelines/example.yml`; ver `tests/test_cli_layers.py`. |
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
- **Ejecuciones reales**: los YAML de ejemplo dejan `dry_run: true` para evitar ejecuciones accidentales. Si se requiere operación
  real, el runbook indica cómo sobreescribir `dry_run` por capa.

## Observaciones adicionales

- `samples/toy_customers.csv` y su README documentan la generación del dataset de humo.
- GitGuardian continúa activo en GitHub; la documentación explica cómo revisar el check de secretos.
- El pipeline puede etiquetarse como `v0.2.0` una vez que estos guardas se encuentren en la rama base `feature/main-codex`.
