# Initial assessment

## 1. Directorios clave y dependencias

### `src/datacore`
- `cli.py` expone comandos `run-layer`, `run-pipeline` y `validate` sobre Typer, cargando configuraciones YAML y aplicando `LayerRuntimeConfigModel` antes de delegar a los runners por capa.【F:src/datacore/cli.py†L1-L195】
- `config/schema.py` define el contrato completo con Pydantic para cómputo, IO, autenticación HTTP/JDBC y bloques de capa reutilizables, lo que introduce dependencias en `pydantic` y en el tipado estructurado de los YAML.【F:src/datacore/config/schema.py†L1-L200】
- `context.py` construye `PipelineContext`, exige rutas absolutas cuando `dry_run` es falso y crea sesiones Spark/administradores de base de datos opcionales, acoplando la ejecución con `yaml`, `pyspark` y gestores JDBC externos.【F:src/datacore/context.py†L1-L196】
- Paquete `io/` agrupa conectores hacia HTTP (`requests` + paginación, retries e incrementales) y archivos (`fsspec`, pandas, polars y Spark cuando está disponible), gestionando credenciales a partir de la configuración de entorno.【F:src/datacore/io/http.py†L1-L200】【F:src/datacore/io/fs.py†L1-L157】
- Capas `raw/bronze/silver/gold` unifican merges de configuración y rutas preferidas, reutilizando `PipelineContext` y Spark para determinar URIs y watermarks; son los destinos principales de los runners invocados desde la CLI.【F:src/datacore/cli.py†L13-L28】【F:src/datacore/context.py†L28-L155】

### `cfg/`
- Carpeta de plantillas operativas segmentadas por capa (`raw`, `bronze`, `silver`, `gold`) con variantes `example`, `template` y `*.prod.yml` por nube; los ejemplos ahora apuntan a URIs remotas y sirven como base para `prodi plan/run` en lugar de depender de rutas locales.【F:cfg/raw/example.yml†L1-L32】【F:cfg/bronze/example.yml†L1-L20】
- Configuraciones productivas (`aws.prod.yml`, `azure.prod.yml`, `gcp.prod.yml`) alimentan los jobs descritos en `docs/run/**` y se sincronizan a buckets según cada nube.【F:docs/run/aws.md†L21-L68】【F:docs/run/gcp.md†L20-L73】

### `config/`
- `env.yml` centraliza credenciales indirectas para S3/ABFSS/GCS vía variables de entorno, reflejando los parámetros que `fs.py` espera al construir `fsspec` storage options.【F:config/env.yml†L1-L23】【F:src/datacore/io/fs.py†L86-L157】
- `database.yml` define escenarios PostgreSQL (local, Docker, producción parametrizada), usados por `build_context` al activar la capa `gold` con catálogo relacional.【F:config/database.yml†L1-L74】【F:src/datacore/context.py†L106-L155】

### `docs/`
- El README y los cuadernos de `docs/run/**` documentan cómo sincronizar los `*.prod.yml` por nube sin depender de `/dbfs/**`; el flujo legacy permanece en [`docs/legacy/`](../legacy/).【F:README.md†L1-L40】【F:docs/run/aws.md†L6-L96】

## 2. Supuestos de filesystem y credenciales
- Los YAML de ejemplo eliminan `file://` y `use_local_fallback` por defecto; cualquier prueba local requiere overrides dinámicos o la futura CLI `prodi plan/run` para generar escenarios controlados.【F:cfg/raw/example.yml†L1-L32】【F:README.md†L21-L33】
- El contenido basado en `/dbfs/configs/datacore/**` pasó a `docs/legacy/` a la espera del generador `prodi plan`; la documentación activa hace referencia directa a los `*.prod.yml` por nube.【F:docs/azure-databricks.md†L1-L3】【F:README.md†L17-L33】【F:docs/run/aws.md†L6-L96】
- `config/env.yml` delega en variables estándar (`AWS_ACCESS_KEY_ID`, `AZURE_STORAGE_ACCOUNT_NAME`, `GOOGLE_APPLICATION_CREDENTIALS`, etc.) para obtener credenciales, y `fs.py` rechaza configuraciones que intenten desactivar TLS, por lo que la operación depende de que esos secretos existan en tiempo de ejecución.【F:config/env.yml†L3-L23】【F:src/datacore/io/fs.py†L96-L140】
- El dataset de ejemplo mantiene `local_fallback` exclusivamente para pruebas asistidas por la CLI, pero los pipelines productivos dependen de URIs remotas (`s3://`, `abfss://`, `gs://`).【F:config/datasets/examples/toy_customers.yml†L1-L48】

## 3. Matriz de compatibilidad y riesgos

| Área | Local (desarrollo) | Azure Databricks | AWS Glue/EMR | GCP Dataproc |
| --- | --- | --- | --- | --- |
| Empaquetado e instalación | Editable desde la raíz del repo (`pip install -e .`) con dependencias (`pyyaml`, `typer`, `pydantic`, conectores fsspec).【F:README.md†L1-L17】 | Transición a `prodi plan/run`; la guía legacy conserva `%pip install -e .` como referencia histórica.【F:docs/azure-databricks.md†L1-L3】【F:docs/legacy/azure-databricks.md†L1-L40】 | Wheel `mvp_config_driven-0.2.x` cargado en S3 y referenciado por Glue/EMR jobs.【F:docs/run/aws.md†L6-L80】 | Wheel publicado en GCS y usado por `gcloud dataproc jobs submit pyspark`.【F:docs/run/gcp.md†L6-L72】 |
| Configuración runtime | YAML de ejemplo con URIs remotas (`s3://`, `abfss://`, `gs://`) y `dry_run` activado por defecto; las ejecuciones reales requieren rutas absolutas y overrides controlados.【F:cfg/raw/example.yml†L1-L32】 | Guías legacy con `/dbfs/...` archivadas a la espera de `prodi plan`; usar `*.prod.yml` actuales para despliegues reales.【F:docs/azure-databricks.md†L1-L3】【F:docs/legacy/azure-databricks.md†L1-L156】【F:README.md†L17-L33】 | Jobs referencian `cfg/<layer>/aws.prod.yml` desde S3 y permiten overrides via `--env.PRODI_FORCE_DRY_RUN`.【F:docs/run/aws.md†L21-L96】 | Workflow template encadena `cfg/<layer>/gcp.prod.yml` desde GCS, con override `--env.PRODI_FORCE_DRY_RUN=true`.【F:docs/run/gcp.md†L20-L93】 |
| Motor de ejecución | Spark opcional: `ensure_spark` falla si `pyspark` no está instalado cuando `dry_run` es falso.【F:src/datacore/context.py†L158-L177】 | Operación en Databricks en revisión; la guía legacy describe requisitos de Runtime y quedará reemplazada por `prodi plan` + jobs gestionados.【F:docs/azure-databricks.md†L1-L3】【F:docs/legacy/azure-databricks.md†L9-L156】 | Glue/EMR Jobs PySpark; Step Functions orquestan cada capa con el mismo wheel.【F:docs/run/aws.md†L21-L80】 | Dataproc PySpark clusters o serverless ejecutan cada capa en secuencia.【F:docs/run/gcp.md†L18-L72】【F:docs/run/gcp.md†L95-L103】 |
| Gestión de credenciales | Variables locales `.env` o exportadas según `config/env.yml`. | Secret Scope y configuraciones ABFSS permanecen documentadas sólo en la guía legacy mientras se implementa `prodi plan`.【F:docs/azure-databricks.md†L1-L3】【F:docs/legacy/azure-databricks.md†L33-L84】 | IAM/Parameter Store; no se embeben llaves en YAML (`cfg/*/aws.prod.yml`).【F:docs/run/aws.md†L98-L105】 | Cuentas de servicio gestionadas y parámetros del workflow; sin llaves en YAML.【F:docs/run/gcp.md†L95-L103】 |

**Riesgos identificados**
1. Dependencia dura en Spark para ejecuciones reales: si `pyspark` no está disponible, `ensure_spark` aborta incluso para pipelines con IO alternativo.【F:src/datacore/context.py†L158-L177】
2. Credenciales indirectas: cualquier variable faltante (`AWS_ACCESS_KEY_ID`, `AZURE_STORAGE_ACCOUNT_KEY`, etc.) rompe la resolución de `storage_options_from_env`; además se rechaza desactivar TLS, por lo que configuraciones heredadas con `S3A_DISABLE_SSL` fallarán.【F:config/env.yml†L3-L23】【F:src/datacore/io/fs.py†L96-L140】
3. Aún no existe un generador consolidado de configuraciones (`prodi plan/run`); mientras llega, cada equipo debe clonar los `*.prod.yml` manualmente, con riesgo de desviaciones entre entornos.【F:README.md†L17-L33】【F:docs/run/aws.md†L54-L69】
4. Conectores HTTP dependen de librerías opcionales (`pandas`, `polars`) para normalizar datos; en entornos limitados sin estas dependencias la ejecución puede degradar o fallar según los caminos de código.【F:src/datacore/io/http.py†L12-L29】

## 4. Plan por fases y candidatos a deprecación

### Fase 1 — Normalización de esquemas y validaciones
1. Consolidar reglas de migración y aliasado en `LayerRuntimeConfigModel`, agregando validaciones para rutas absolutas según tipo de despliegue.【F:src/datacore/config/schema.py†L1-L200】
2. Extender `build_context` para reportar métricas claras cuando falten `dataset_config`/`environment_config`, reduciendo errores silenciosos en CI local.【F:src/datacore/context.py†L28-L155】
3. Documentar en README la matriz de compatibilidad y requerimientos de dependencias para cada modo.

### Fase 2 — Endurecimiento de conectores y credenciales
1. Revisar `io.http` para desacoplar normalización de dependencias opcionales, asegurando modos degradados sin pandas/polars.【F:src/datacore/io/http.py†L12-L160】
2. Ampliar `io.fs` con pruebas de integración que cubran S3/ABFSS/GS utilizando los mapas de `config/env.yml`, validando TLS obligatorio y escenarios de fallback.【F:src/datacore/io/fs.py†L1-L157】【F:config/env.yml†L3-L23】
3. Externalizar ejemplos de credenciales (Secret Scopes, IAM) en documentación por nube para reducir duplicidad en YAML.【F:docs/azure-databricks.md†L33-L84】【F:docs/run/aws.md†L98-L105】【F:docs/run/gcp.md†L95-L103】

### Fase 3 — Automatización multi-plataforma
1. Entregar la CLI `prodi plan/run` que genere paquetes listos para Databricks, Glue y Dataproc reutilizando los flujos descritos en `docs/run/**` y sustituyendo las guías legacy.【F:docs/run/aws.md†L6-L80】【F:docs/run/gcp.md†L6-L93】【F:README.md†L17-L33】
2. Incorporar matrices de CI que ejecuten `prodi validate` con `PRODI_FORCE_DRY_RUN` en cada plantilla cloud para detectar regresiones temprano.【F:README.md†L65-L91】
3. Revisar dependencias compartidas para publicar requisitos mínimos (requests, fsspec, pyspark opcional) en `pyproject` y guías de instalación.

### Candidatos a deprecación
- Plantillas `cfg/*/template.yml` dependen de rutas genéricas (`config/datasets/example.yml`) y comentarios heredados; las versiones `example` y `*.prod.yml` cubren los mismos casos con mayor fidelidad.【F:cfg/raw/template.yml†L1-L21】【F:cfg/bronze/template.yml†L1-L18】
- Scripts ad-hoc como `scripts/aws_glue_submit.sh` replican la lógica documentada en Step Functions y podrían reemplazarse por IaC o pipelines consolidados para evitar divergencia.【F:scripts/aws_glue_submit.sh†L1-L46】【F:docs/run/aws.md†L21-L80】
