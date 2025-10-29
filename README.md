# MVP Config-Driven Pipeline

Plataforma modular para ejecutar pipelines Spark por capas (`raw` → `bronze` →
`silver` → `gold`) declarados en YAML. La CLI `prodi`, instalada desde
`lib/datacore`, normaliza las configuraciones con
`LayerRuntimeConfigModel` y aplica el contrato real definido por
`LayerConfig.from_dict` y `build_context`.

## Contrato de configuración

Cuando una capa se ejecuta con `dry_run: false` la configuración **debe**
incluir referencias absolutas a los artefactos publicados en almacenamiento
compartido:

- `dataset_config`: ruta YAML con la definición de dataset. Se resuelve desde
  `io.source.dataset_config` o `io.source.dataset`.
- `environment_config`: ruta YAML con la configuración de entorno. Se busca en
  `io.source.environment_config`, `io.source.environment`, o en la raíz como
  `environment_config`/`env_config`.
- `database_config`: obligatorio sólo cuando la capa `gold` publica metadatos en
  bases de datos. Se ubica en `io.sink.database_config` o `io.sink.database`.

Durante la construcción del contexto, `build_context` valida que las rutas
apuntan a archivos reales, migra el dataset con `DatasetConfigModel` y habilita
el administrador de base de datos cuando procede. Si falta alguno de los
artículos anteriores la ejecución falla antes de tocar datos.

## Plantillas listas para Databricks

El directorio `templates/azure/dbfs/` contiene una versión "copy-paste" de los
archivos necesarios para correr la cadena de capas sobre Databricks y ABFSS:

- `cfg/env.yml`: credenciales `abfss` obtenidas mediante secret scopes y
  mapeadas a `AZURE_STORAGE_ACCOUNT_NAME` / `AZURE_STORAGE_ACCOUNT_KEY`.
- `datasets/toy_customers.yml`: dataset mínimo con rutas `abfss://` por capa,
  checkpoints declarados y `local_fallback` sólo a modo de referencia.
- `cfg/raw.yml`, `cfg/bronze.yml`, `cfg/silver.yml`, `cfg/gold.yml`: ejemplos de
  configuración por capa usando `/dbfs/configs/datacore/...` para las rutas
  `dataset_config`/`environment_config` y `abfss://` para entradas y salidas.

Todos los sinks usan `uris.abfss` y `checkpointLocation` cuando corresponde para
eliminar dependencias en `options.path`. Las rutas de datos emplean una sola
cuenta `<ACCOUNT>` y el mismo contenedor (`landing`) para facilitar el
cableado y evitar errores "No URI available".

## Guía operativa

La guía detallada en [`docs/azure-databricks.md`](docs/azure-databricks.md)
es la fuente de verdad para Azure Databricks. Incluye fragmentos listos para:

1. Sincronizar el repositorio en Databricks Repos y ejecutar
   `%pip install -e lib/datacore`.
2. Configurar el acceso ABFSS con Secret Scopes (`spark.conf.set(...)` +
   variables de entorno).
3. Publicar las plantillas en `/dbfs/configs/datacore/**` con `dbutils.fs.put`.
4. Validar cada capa con `prodi validate -c /dbfs/...` y ejecutar `prodi
   run-layer` en orden Raw → Bronze → Silver → Gold.
5. Leer el Delta final (gold) con Spark (`display(df)` y `df.count()`).

La documentación antigua que asumía wheels monolíticos o rutas locales fue
movida a `docs/legacy/` para consulta histórica.

## Guardarraíles y CI

- `lib/datacore/pyproject.toml` empaqueta la librería para instalaciones
  editables (`pip install -e lib/datacore`).
- `.github/workflows/validate-configs.yml` lintéa los YAML cloud y ejecuta
  `prodi validate` sobre las plantillas `/dbfs/...` después de publicarlas en el
  filesystem del runner.
- `tests/test_cli_layers.py` cubre invariantes de esquema y asegura que los
  ejemplos mantienen `dry_run` donde corresponde.

Ejecuta `pytest` y `prodi validate -c templates/azure/dbfs/cfg/<layer>.yml`
antes de integrar cambios para respetar el contrato de configuración.
