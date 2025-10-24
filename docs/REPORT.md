# Auditoría técnica del refactor multi-nube

## Índice
- [Resumen ejecutivo](#resumen-ejecutivo)
- [Mapa del repositorio y dependencias](#mapa-del-repositorio-y-dependencias)
- [Cross-layer calls](#cross-layer-calls)
- [Hallazgos por categoría](#hallazgos-por-categoría)
  - [Capas](#capas)
  - [CLI y empaquetado](#cli-y-empaquetado)
  - [I/O y portabilidad](#io-y-portabilidad)
  - [Seguridad](#seguridad)
  - [Configuraciones por capa](#configuraciones-por-capa)
  - [Notebooks](#notebooks)
  - [CI / Tests / Cobertura](#ci--tests--cobertura)
- [Evidencia destacada](#evidencia-destacada)
- [Riesgos y prioridad](#riesgos-y-prioridad)
- [Plan sugerido de correcciones](#plan-sugerido-de-correcciones)
- [Anexos](#anexos)

## Resumen ejecutivo
- Las capas están fuertemente acopladas en cascada (`gold → silver → bronze → raw`) y la CLI perpetúa la cadena; no existe aislamiento real por capa.
- El CLI moderno (`prodi run-layer`) existe, pero el empaquetado carece de `build-system` y convive con scripts monolíticos heredados (`pipelines/spark_job.py`, `spark_job_with_db.py`).
- El stack de I/O usa `fsspec` para lecturas/escrituras portables, pero los helpers S3A heredados siguen configurando `spark.hadoop.fs.s3a.*` y permiten desactivar TLS.
- Se encontraron logs y scripts que imprimen credenciales y contraseñas (AWS keys, postgres) y deshabilitan SSL explícitamente.
- La configuración por capa en `cfg/` son plantillas mínimas; los datasets reales en `config/datasets/**` no siguen un esquema armonizado (`compute`, `io.source/sink`, `transform.sql`, `dq`).
- No hay notebooks en el repositorio, pero los scripts `run_*` mantienen lógica de orquestación pesada fuera de `src/`.
- La CI sólo corre linting de configuraciones; no hay build de wheel ni ejecución de tests. Los tests existentes son unitarios mínimos.

## Mapa del repositorio y dependencias

### Árbol de carpetas principales
```
.
├─ cfg/ (plantillas por capa)
├─ config/ (datasets, env, database)
├─ data/ (datasets de ejemplo y staging)
├─ docs/ (documentación, reportes)
├─ pipelines/ (scripts Spark heredados y utilidades)
├─ scripts/ (wrappers operativos, generación de datos)
├─ src/datacore/ (núcleo del refactor)
└─ tests/ (pruebas unitarias puntuales)
```

### Grafo de dependencias
- Ver [`docs/diagrams/dependencies.md`](diagrams/dependencies.md) para el grafo Mermaid actualizado.
- El flujo de ejecución de capas y orquestadores se resume en [`docs/diagrams/pipeline_flow.md`](diagrams/pipeline_flow.md).

## Cross-layer calls
Cada capa ejecuta la inmediatamente anterior cuando no recibe un `DataFrame` de entrada. Esto rompe el aislamiento buscado.

**Silver → Bronze** (`src/datacore/layers/silver/main.py`)
```python
 5: from datacore.layers.bronze.main import execute as execute_bronze
10: def execute(context: PipelineContext, spark, df=None):
11:     if df is None:
12:         df = execute_bronze(context, spark)
```

**Bronze → Raw** (`src/datacore/layers/bronze/main.py`)
```python
 6: from datacore.layers.raw.main import execute as execute_raw
10: def execute(context: PipelineContext, spark, df=None):
11:     if df is None:
12:         df = execute_raw(context, spark)
```

**Gold → Silver** (`src/datacore/layers/gold/main.py`)
```python
 7: from datacore.layers.silver.main import execute as execute_silver
15: def execute(context: PipelineContext, spark, df=None):
16:     if df is None:
17:         df = execute_silver(context, spark)
```

La CLI y los jobs heredados también encadenan todas las capas de forma secuencial.

## Hallazgos por categoría

### Capas
- Los módulos `datacore.layers.*` delegan la mayor parte de la lógica a `datacore.pipeline.utils`, pero mantienen imports cruzados que ejecutan capas previas si no se les entrega un `DataFrame`. Esto impide ejecutar una capa aislada para pruebas o para pipelines parciales.
- `datacore.pipeline.utils` contiene lógica compartida de transforms, calidad y escrituras que depende fuertemente de Spark y de rutas `s3a://` provenientes de la configuración.

### CLI y empaquetado
- `pyproject.toml` define el script `prodi = "datacore.cli:main"`, pero no declara un bloque `[build-system]`, por lo que no se puede construir un wheel estándar.
- `datacore/cli.py` implementa `run-layer` con Typer y respeta la secuencia Raw→Bronze→Silver→Gold incluso si sólo se quiere ejecutar Silver.
- Persiste `pipelines/spark_job.py` y `pipelines/spark_job_with_db.py`, scripts monolíticos que orquestan toda la cadena fuera de `src/`.
- `scripts/runner.sh` y `scripts/runner_with_db.sh` siguen invocando `spark_job_with_db.py` y configuran Spark vía CLI, evidenciando deuda técnica del orquestador anterior.

### I/O y portabilidad
- Lecturas:
  - `pipelines/sources.load_source` usa `datacore.io.fs.read_df` (fsspec) para `s3://`, `abfss://`, `gs://`, pero hace fallback a `spark.read.format("jdbc")` para fuentes JDBC.
  - `pipelines/spark_job.py` continúa leyendo directamente con `spark.read.csv/json/parquet` sobre rutas `s3a://`.
- Escrituras:
  - `run_bronze_stage` escribe `parquet` en la ruta configurada (`s3a://...` por defecto) y relanza la lectura desde el mismo path.
  - `run_silver_stage` escribe al sink configurado (`save(out["path"])`) con soporte para `overwrite_dynamic`.
  - `write_to_gold_bucket` soporta `parquet/json/csv` con `df.write.save(path)` tras configurar S3A.
  - La cuarentena de calidad (`pipelines.validation.quality.apply_quality`) escribe en modo append `parquet` al `quarantine_path`.
- Non-portable:
  - `pipelines.common.maybe_config_s3a` configura claves `spark.hadoop.fs.s3a.*` y desactiva TLS si la configuración lo pide.
  - Los scripts `runner*.sh` hardcodean `spark.hadoop.fs.s3a.connection.ssl.enabled=false` y credenciales MinIO.

### Seguridad
- Logs de credenciales: `scripts/run_high_volume_case.py` imprime `Set AWS_ACCESS_KEY_ID=minioadmin` y similares. `scripts/runner_with_db.sh` hace `echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"`.
- TLS desactivado: `pipelines.common.maybe_config_s3a` puede escribir `spark.hadoop.fs.s3a.connection.ssl.enabled=false`; los runners de Spark lo forzan por defecto.
- Contraseñas en texto plano: `config/database.yml` mantiene `postgres123` para entornos default y development.

### Configuraciones por capa
- Plantillas `cfg/{raw,bronze,silver,gold}/template.yml` sólo contienen `dry_run`, `layer` y rutas a configs, sin validar claves mínimas.
- Los datasets en `config/datasets/**/dataset*.yml` tienen secciones `source`, `standardization`, `quality`, `output`, pero no usan un esquema común basado en `compute`, `io.source`, `io.sink`, `transform.sql|udf`, `dq`.
- Se detecta mezcla de secciones `output.bronze` y `output.gold` con estructuras distintas por dataset, lo que complica validaciones automáticas.

### Notebooks
- No se encontraron notebooks en el repositorio (`find . -name '*.ipynb'` sin resultados). La lógica de exploración y orquestación vive en scripts Python/Shell.
- Se recomienda migrar la lógica de `scripts/run_*` a comandos Typer o módulos en `src/` para reducir deuda fuera del paquete.

### CI / Tests / Cobertura
- `ci/lint.yml` sólo ejecuta `./ci/check_config.sh` (yamllint + jq). No hay pipelines que ejecuten `pytest` ni build de artefactos.
- Tests existentes: `tests/test_cli_smoke.py` (smoke de CLI) y `tests/test_common.py` / `tests/test_io_fs.py` (helpers). No cubren rutas críticas de I/O multi-nube ni capas Bronze/Silver/Gold.
- No hay reporte de cobertura configurado (aunque `pytest-cov` está en `requirements.txt`).

## Evidencia destacada
- CLI secuencial: ver `src/datacore/cli.py` (líneas 49-60) donde se recorre `_stage_sequence`.
- Scripts heredados: `pipelines/spark_job_with_db.py` ejecuta Raw→Bronze→Silver→Gold dentro de `main()`.
- Configuración S3A y desactivación de SSL: `pipelines/common.py` líneas 155-185.
- Escritura Silver: `datacore/pipeline/utils.py` líneas 373-421 (`writer.save(out["path"])`).
- Escritura Gold bucket: `datacore/pipeline/utils.py` líneas 541-604.
- Quarantine write heredada: `pipelines/validation/quality.py` líneas 84-92.
- Logs de credenciales: `scripts/run_high_volume_case.py` líneas 26-40 y `scripts/runner_with_db.sh` líneas 35-45.
- TLS deshabilitado en runners: `scripts/runner_with_db.sh` líneas 47-58 y `scripts/runner.sh` líneas 75-87.
- Config datasets sin claves `compute/io`: `config/datasets/finanzas/payments_v1/dataset.yml` (estructura custom).
- CI limitada: `ci/lint.yml` sólo lanza `check_config.sh`.

## Riesgos y prioridad
| Riesgo | Descripción | Impacto | Prioridad |
| --- | --- | --- | --- |
| Cross-layer acoplado | Silver/Bronze/Gold importan y ejecutan capas anteriores. | Imposibilita despliegues parciales, aumenta riesgo de regresiones. | P0 |
| Scripts heredados vs CLI | `spark_job.py` y runners siguen operativos fuera del paquete. | Doble camino operativo, difícil gobernanza. | P0 |
| Desactivación de TLS | `maybe_config_s3a` y runners fijan `ssl.enabled=false`. | Compromete seguridad en nubes públicas. | P0 |
| Logs de secretos | Scripts imprimen AWS keys y contraseñas. | Exposición de credenciales. | P0 |
| Config heterogénea | Datasets no siguen esquema `compute/io`. | Dificulta validaciones y automatización multi-nube. | P1 |
| Falta de CI/tests | No se ejecutan tests ni builds. | Riesgo de regresiones sin detección temprana. | P1 |
| Paquete sin build-system | No se puede publicar wheel limpio. | Bloquea distribución estandarizada. | P1 |
| Portabilidad incompleta | Helpers S3A heredados dominan rutas `s3a://`. | Limita adopción real multi-nube si no se normaliza a URIs genéricas. | P2 |

## Plan sugerido de correcciones
1. **Cortar dependencias entre capas**: refactorizar `execute` para requerir `df` explícito y mover orquestación al CLI (o a un planificador) con flujos configurables. Añadir tests de integración por capa.
2. **Unificar entrypoints**: deprecar `pipelines/spark_job*.py` y exponer comandos Typer (`prodi run-layer`, `prodi run-pipeline`) encapsulados en `src/`. Actualizar runners a usar la CLI.
3. **Endurecer I/O multi-nube**: reemplazar `maybe_config_s3a` por adaptadores fsspec, habilitar TLS por defecto y parametrizar endpoints/credenciales via `storage_options_from_env`.
4. **Sanear seguridad**: eliminar logs de claves, cargar secretos desde vault/ENV sin imprimirlos, y remover `ssl.enabled=false` por defecto. Revisar `config/database.yml` para manejar secretos cifrados.
5. **Normalizar configuraciones**: definir esquema YAML común (`compute`, `io.source`, `io.sink`, `transform`, `dq`) y validar con `jsonschema`. Ajustar plantillas `cfg/` y datasets reales.
6. **Fortalecer CI**: agregar workflow que instale dependencias, construya wheel (`build`/`pip install .`) y ejecute `pytest --cov`. Añadir suites por capa y por adaptadores de I/O.

## Anexos

### Tabla de lecturas/escrituras
| Tipo | Archivo / Función | URI / Destino | Formato | Engine |
| --- | --- | --- | --- | --- |
| Lectura | `pipelines/sources.py::load_source` | `cfg['source']['path']` (ej. `s3a://...`) | csv/json/jsonl/parquet | `read_df` (fsspec + Spark/Pandas/Polars) |
| Lectura | `pipelines/sources.py::load_source` (branch JDBC) | `jdbc.url` | jdbc | `spark.read.format("jdbc")` |
| Lectura | `pipelines/spark_job.py::main` | `cfg['source']['path']` | csv/json/parquet | `spark.read` |
| Lectura | `datacore/pipeline/utils.run_bronze_stage` | `bronze_path` | parquet | `spark.read.parquet` |
| Escritura | `datacore/pipeline/utils.run_bronze_stage` | `bronze_path` | parquet | `df.write.parquet` |
| Escritura | `datacore/pipeline/utils.run_silver_stage` | `out['path']` | configurable (default parquet) | `df.write.save` |
| Escritura | `datacore/pipeline/utils.write_to_gold_bucket` | `bucket_cfg['path']` | parquet/json/csv | `df.write.save` |
| Escritura | `datacore/pipeline/utils.write_to_gold_database` | `db_manager.write_dataframe` | tabla relacional | ORM/JDBC |
| Escritura | `pipelines.validation.quality.apply_quality` | `quarantine_path` | parquet | `bad_df.write.mode('append').parquet` |

### Configuraciones por capa
- `cfg/raw/template.yml`, `cfg/bronze/template.yml`, `cfg/silver/template.yml`, `cfg/gold/template.yml` – plantillas básicas sin claves de I/O/compute/dq.
- Datasets reales:
  - `config/datasets/finanzas/payments_v1/dataset.yml`
  - `config/datasets/finanzas/payments_db_only/dataset.yml`
  - `config/datasets/finanzas/payments_multi/dataset.yml`
  - `config/datasets/casos_uso/events_multiformat.yml`
  - `config/datasets/casos_uso/payments_high_volume.yml`

### Resultados de búsqueda clave
- `rg "datacore.layers" src/datacore` → identifica imports cruzados.
- `rg "s3a://"` → rutas S3A hardcodeadas en configs, scripts y tests.
- `rg "ssl.enabled=false"` → ubicaciones que deshabilitan TLS.
- `find . -name '*.ipynb'` → confirma ausencia de notebooks.
- `ci/check_config.sh` → único chequeo automatizado en CI.

