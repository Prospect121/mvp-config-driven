# Auditoría técnica – Estado actual del pipeline config-driven

## Resumen ejecutivo
- El pipeline sigue siendo un script monolítico (`pipelines/spark_job_with_db.py`) que ejecuta de punta a punta las etapas Raw→Bronze→Silver→Gold dentro de un solo `main`, sin módulos `layers/*` ni funciones `run(cfg)` separadas.【F:pipelines/spark_job_with_db.py†L515-L915】
- La configuración de S3A expone secretos y desactiva TLS en múltiples lugares (`maybe_config_s3a` y los runners de Spark), incumpliendo los criterios de seguridad para el refactor multi-nube.【F:pipelines/common.py†L128-L149】【F:scripts/runner.sh†L70-L87】【F:scripts/runner_with_db.sh†L35-L58】
- Todo el I/O depende de rutas `s3a://` y de la API nativa de Spark; no existe abstracción fsspec ni soporte probado para `abfss://` o `gs://`, por lo que la portabilidad de storage es nula actualmente.【F:pipelines/sources.py†L63-L252】【F:pipelines/spark_job_with_db.py†L573-L777】【F:pipelines/validation/quality.py†L1-L67】
- No hay CLI `prodi` ni empaquetado de wheel; el `pyproject.toml` solo contiene herramientas de linting y no registra entrypoints ejecutables.【F:pyproject.toml†L1-L20】
- Las configuraciones YAML están centralizadas en `config/datasets/**` y mezclan definiciones de todas las capas en un solo archivo; no existen `cfg/raw/*.yml`/`cfg/bronze/*.yml` separados según la arquitectura objetivo.【F:config/datasets/finanzas/payments_v1/dataset.yml†L1-L45】【F:config/datasets/finanzas/payments_v3/dataset.yml†L8-L89】

## 1. Mapa de capas y dependencias

### Árbol de carpetas (top 2 niveles relevantes)
```
config/                  # env.yml, database.yml y datasets/* (YAML por dataset)
data/                    # muestras locales, staging s3a
pipelines/               # lógica completa del pipeline (common, sources, io, validation, transforms, spark_job*.py)
  ├── spark_job.py
  ├── spark_job_with_db.py
  ├── io/ (reader.py, writer.py, s3a.py)
  ├── validation/ (quality.py)
  ├── transforms/ (apply.py, tests/)
  ├── database/ (db_manager.py, schema_mapper.py)
  ├── utils/ (logger.py, parallel.py)
  ├── config/ (loader.py)
  └── sources.py
scripts/                 # runners (bash/python) y generadores de datos
ci/                      # scripts y workflows de ejemplo (sin .github/)
docs/                    # documentación actual y nuevos reportes
```

### Grafo de imports entre módulos
- Ver `docs/diagrams/dependency_graph.md` para el diagrama Mermaid generado.
- Núcleo monolítico: `pipelines.spark_job_with_db` depende de `pipelines.sources`, `pipelines.transforms.apply`, `pipelines.validation.quality`, `pipelines.database.*` y `pipelines.common` (S3A, casts, ordenamientos).【F:pipelines/spark_job_with_db.py†L573-L777】
- `pipelines.sources` y `pipelines.io.s3a` solo ofrecen wrappers sobre `maybe_config_s3a`, reforzando el acoplamiento a S3A.【F:pipelines/sources.py†L63-L252】【F:pipelines/io/s3a.py†L1-L7】
- Las dependencias transversales (`pipelines.utils.logger`, `pipelines.common.safe_cast`) se usan en todas las etapas, pero no hay separación por capa.

### Cross-layer calls
No existen paquetes `layers/raw|bronze|silver|gold`; todo sucede en `pipelines/spark_job_with_db.main`. No se encontraron imports `from layers.` ni llamadas `layers.silver`→`layers.bronze` porque las capas aún no están separadas (resultado esperado de la auditoría).【F:pipelines/spark_job_with_db.py†L573-L915】

## 2. Entry points y empaquetado
- Entrypoints operativos:
  - `pipelines/spark_job_with_db.py` y la variante `pipelines/spark_job.py` (sin DB). Ambos se ejecutan vía `spark-submit` desde scripts o Docker.【F:pipelines/spark_job_with_db.py†L515-L915】【F:scripts/runner.sh†L70-L87】
  - Scripts auxiliares: `scripts/runner.sh`, `scripts/runner_with_db.sh`, `scripts/run_multiformat_case.py`, `scripts/run_high_volume_case.py` (todos llaman directamente a `spark_job_with_db.py`).【F:scripts/run_multiformat_case.py†L71-L118】【F:scripts/run_high_volume_case.py†L71-L129】
  - Docker Compose orquesta Spark + MinIO + Postgres y lanza `/mvp/scripts/runner.sh` por defecto.【F:docker-compose.yml†L1-L108】
- `pyproject.toml` carece de `[project]`, `[project.scripts]` o configuración de wheel; no existe CLI `prodi run-layer` ni módulo `src/datacore`.【F:pyproject.toml†L1-L20】
- No se encontró `setup.cfg` ni `setup.py`; el repositorio no se empaqueta actualmente.

## 3. I/O y portabilidad de storage
- Lecturas/escrituras usan únicamente Spark (`spark.read`, `df.write`) con rutas `s3a://`; no hay abstracción fsspec ni drivers alternativos para `abfss://` o `gs://`.【F:pipelines/sources.py†L63-L252】【F:pipelines/spark_job_with_db.py†L573-L777】
- `pipelines/common.maybe_config_s3a` y `pipelines/io/s3a.configure_s3a` son los únicos helpers de storage; ambos codifican parámetros S3A y retornan inmediatamente si la URI no empieza con `s3a://`.【F:pipelines/common.py†L128-L149】【F:pipelines/io/s3a.py†L1-L7】
- El staging de APIs también escribe con `df.write.parquet/json` sobre `s3a://` en `pipelines/sources.load_source`.【F:pipelines/sources.py†L218-L246】
- Tabla detallada de operaciones en Anexos.

## 4. Seguridad (secretos y TLS)
- `maybe_config_s3a` imprime `AWS_ACCESS_KEY_ID` y configura `spark.hadoop.fs.s3a.connection.ssl.enabled=false`, exponiendo credenciales y forzando HTTP.【F:pipelines/common.py†L128-L147】
- `scripts/runner.sh` y `scripts/runner_with_db.sh` exportan credenciales en texto plano, las loguean (`echo "AWS_ACCESS_KEY_ID"`) y desactivan TLS en los flags de `spark-submit`.【F:scripts/runner.sh†L70-L87】【F:scripts/runner_with_db.sh†L35-L58】
- Los scripts Python de casos de uso establecen variables AWS hardcodeadas (`minioadmin`).【F:scripts/run_multiformat_case.py†L24-L38】【F:scripts/run_high_volume_case.py†L26-L40】
- `docker-compose.yml` incluye secretos por defecto (MinIO root, Postgres password) y utiliza MinIO sin TLS.【F:docker-compose.yml†L12-L87】

## 5. Configuración por capa (YAML)
- No existen `cfg/raw/*.yml`/`cfg/silver/*.yml`. Todos los datasets definen `source`, `standardization`, `quality`, `output.silver` y `output.gold` dentro de un único archivo, mezclando lógica de todas las capas.【F:config/datasets/finanzas/payments_v1/dataset.yml†L1-L45】【F:config/datasets/casos_uso/events_multiformat.yml†L1-L59】
- `config/env.yml` solo tiene `timezone` y `s3a_endpoint`; no hay banderas para otros backends (ADLS/GCS).【F:config/env.yml†L1-L2】
- `config/database.yml` concentra configuración de Gold (PostgreSQL) y transformaciones por defecto.【F:config/database.yml†L5-L85】

## 6. Notebooks
- No se encontraron notebooks (`find . -name '*.ipynb'` vacío). La lógica de negocio vive en scripts Python/Bash, no en notebooks.

## 7. CI/Tests y cobertura
- No hay `.github/workflows`. `ci/lint.yml` es un ejemplo aislado que ejecuta `yamllint` y `jq`, pero no está registrado en GitHub Actions al faltar el directorio `.github`.【F:ci/lint.yml†L1-L16】
- Tests existentes: solo `pipelines/transforms/tests/test_apply.py`, que valida transformaciones SQL básicas. No hay pruebas para I/O, CLI ni pipelines completos.【F:pipelines/transforms/tests/test_apply.py†L1-L27】
- Cobertura no reportada; tampoco hay `pytest` ejecutado automáticamente.

## 8. Estado vs objetivos del refactor

| Objetivo | Estado actual | Evidencia | Riesgo |
| --- | --- | --- | --- |
| Capas separadas (raw/bronze/silver/gold) | ❌ Monolítico en `spark_job_with_db.py` | `main` ejecuta todas las etapas y escribe Silver/Gold directamente.【F:pipelines/spark_job_with_db.py†L573-L915】 | P0 – dificulta reintentos y orquestación.
| CLI `prodi run-layer` | ❌ Inexistente; no hay empaquetado | `pyproject.toml` sin `[project]` ni scripts.【F:pyproject.toml†L1-L20】 | P0 – requisito principal.
| I/O multi-nube con fsspec | ❌ Solo S3A y Spark nativo | `maybe_config_s3a` + rutas `s3a://` hardcodeadas.【F:pipelines/common.py†L128-L149】【F:pipelines/sources.py†L63-L252】 | P0 – bloquea portabilidad.
| Seguridad (sin secretos en logs, TLS activo) | ❌ Credenciales logueadas, TLS off | `print` de claves y `connection.ssl.enabled=false` en varios scripts.【F:pipelines/common.py†L133-L147】【F:scripts/runner.sh†L70-L87】 | P0 – incumple políticas.
| Empaquetado wheel + orquestación | ❌ Sin wheel, sin docs por nube | Docker Compose + scripts locales únicamente.【F:docker-compose.yml†L1-L108】 | P1 – frena despliegue multi-plataforma.
| Backward compatibility de transformaciones | ⚠️ Transformaciones existentes siguen en `spark_job_with_db`, pero sin modularidad | `apply_sql_transforms`/`apply_udf_transforms` aún se invocan desde el mismo script.【F:pipelines/spark_job_with_db.py†L709-L717】 | P1 – riesgo de regresión al refactorizar.

## 9. Evidencia destacada
- `maybe_config_s3a` imprime la clave y fuerza HTTP.【F:pipelines/common.py†L133-L147】
- `runner.sh` pasa credenciales por línea de comandos y desactiva TLS.【F:scripts/runner.sh†L70-L87】
- El pipeline Silver→Gold se ejecuta en serie dentro del mismo `main`, sin separación de capas.【F:pipelines/spark_job_with_db.py†L573-L877】
- Los datasets combinan configuraciones de múltiples capas en un solo YAML, p. ej. `payments_v3`.【F:config/datasets/finanzas/payments_v3/dataset.yml†L8-L89】

## 10. Riesgos y prioridad
- **P0**: Exposición de credenciales/TLS desactivado (`maybe_config_s3a`, scripts de ejecución).【F:pipelines/common.py†L133-L147】【F:scripts/runner_with_db.sh†L35-L58】
- **P0**: Falta de separación de capas y CLI impide cumplir la arquitectura objetivo y dificulta orquestadores (Databricks/Glue/etc.).【F:pipelines/spark_job_with_db.py†L515-L915】
- **P0**: Dependencia exclusiva de `s3a://` impide portabilidad a ADLS/GCS; no existe fsspec ni mocking para tests.【F:pipelines/sources.py†L63-L252】
- **P1**: Configuración YAML mezclada requiere migración/alias para evitar romper compatibilidad cuando se separen capas.【F:config/datasets/finanzas/payments_v1/dataset.yml†L1-L45】
- **P1**: Tests insuficientes; no cubren ingestion, calidad ni escrituras, lo que dificulta detectar regresiones en el refactor.【F:pipelines/transforms/tests/test_apply.py†L1-L27】
- **P2**: Scripts auxiliares generan datasets grandes con secretos embebidos (`minioadmin`), aptos solo para entornos locales.【F:scripts/generate_synthetic_data.py†L100-L121】

## 11. Plan sugerido (PRs incrementales)
1. **PR seguridad inmediata**: eliminar `print` de credenciales, habilitar TLS por defecto y documentar variables/secret scopes; ajustar scripts `runner*.sh`.【F:pipelines/common.py†L133-L147】【F:scripts/runner.sh†L70-L87】
2. **PR scaffold librería**: crear `pyproject.toml` con metadatos, `src/datacore/cli.py` (Typer) y empaquetado básico; mover helpers puros (`common`, `utils`, `transforms`, `validation`) bajo `src/datacore`.【F:pipelines/common.py†L1-L149】【F:pipelines/transforms/apply.py†L1-L58】
3. **PR separación por capas**: extraer funciones `run_raw/bronze/silver/gold` en `layers/*/main.py`, manteniendo transformaciones; proveer YAML por capa con alias para los actuales. Validar equivalencia con snapshot tests mínimos.【F:pipelines/spark_job_with_db.py†L573-L915】【F:config/datasets/finanzas/payments_v3/dataset.yml†L8-L89】
4. **PR fsspec + Spark factory**: introducir `datacore/io/fs.py` sobre fsspec y `compute/spark.py` para instanciar Spark según URI; reemplazar llamadas directas a `spark.read/write` en `pipelines/sources` y writers. Añadir tests con URIs simuladas `s3://`, `abfss://`, `gs://`.【F:pipelines/sources.py†L63-L252】【F:pipelines/io/writer.py†L1-L33】
5. **PR orquestación/documentación**: agregar workflows CI (`.github/workflows/ci.yml`), scripts `make test`, artefacto wheel y guías Databricks/AWS/GCP/Azure. Completar documentación de ejecución multi-nube. 

## Anexos

### Tabla de lecturas y escrituras

| Tipo | Archivo / Función | URI/Origen | Formato | Engine |
| --- | --- | --- | --- | --- |
| Lectura | `pipelines/sources.load_source` | `cfg['source']['path']` (ej. `s3a://raw/...`) | CSV/JSON/Parquet/JDBC/API | Spark DataFrame【F:pipelines/sources.py†L63-L111】 |
| Lectura | `pipelines/spark_job_with_db.main` (Bronze reload) | `bronze_path` (`s3a://` desde config) | Parquet | Spark DataFrame【F:pipelines/spark_job_with_db.py†L587-L636】 |
| Lectura | `pipelines/validation/quality.apply_quality` (stats) | `df.filter` | — | Spark DataFrame【F:pipelines/validation/quality.py†L18-L57】 |
| Escritura | `pipelines/sources.load_source` (API staging) | `staging_path` (`s3a://` opcional) | Parquet/JSON | Spark Writer【F:pipelines/sources.py†L218-L246】 |
| Escritura | `pipelines/validation/quality.apply_quality` (quarantine) | `quarantine_path` (`s3a://...`) | Parquet | Spark Writer【F:pipelines/validation/quality.py†L58-L67】 |
| Escritura | `pipelines/spark_job_with_db.main` (Silver) | `out['path']` (`s3a://silver/...`) | Parquet/JSON/CSV | Spark Writer【F:pipelines/spark_job_with_db.py†L761-L777】 |
| Escritura | `pipelines/spark_job_with_db.write_to_gold_bucket` | `bucket_cfg['path']` (`s3a://gold/...`) | Parquet/JSON/CSV | Spark Writer【F:pipelines/spark_job_with_db.py†L454-L509】 |
| Escritura | `pipelines/spark_job_with_db.write_to_gold_database` | JDBC (`DatabaseManager.write_dataframe`) | Tabla PostgreSQL | SQLAlchemy + Spark DF→DB【F:pipelines/spark_job_with_db.py†L420-L447】 |
| Escritura | `pipelines/io/writer.write_parquet` | `path` (`s3a://` o local) | Parquet | Spark Writer【F:pipelines/io/writer.py†L12-L33】 |

### Configs por carpeta
- `config/env.yml`: timezone + endpoint S3A.【F:config/env.yml†L1-L2】
- `config/database.yml`: entornos (default/development/production) y `table_settings` globales.【F:config/database.yml†L5-L85】
- `config/datasets/finanzas/*`: cada dataset incluye `source(s)`, `standardization`, `schema`, `quality`, `output.silver`, `output.gold` en un mismo archivo.【F:config/datasets/finanzas/payments_v3/dataset.yml†L8-L89】
- `config/datasets/casos_uso/*`: estructura similar con expectativas y salidas `s3a://`.【F:config/datasets/casos_uso/events_multiformat.yml†L3-L59】

### Resultados de búsqueda clave
- `rg "connection.ssl.enabled"` → `pipelines/common.py`, `scripts/runner*.sh` desactivan TLS.【F:pipelines/common.py†L142-L147】【F:scripts/runner.sh†L81-L87】
- `rg "AWS_SECRET_ACCESS_KEY"` → credenciales hardcodeadas en scripts y docs.【F:scripts/run_high_volume_case.py†L26-L35】
- `rg "prodi"` → sin resultados, confirmando ausencia del CLI solicitado.

### Diagramas
- `docs/diagrams/dependency_graph.md`: grafo Mermaid de imports internos.
- `docs/diagrams/pipeline_flow.md`: flujo actual Raw→Silver→Gold dentro de un solo job.
