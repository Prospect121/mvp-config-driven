# MVP Config-Driven Pipeline

Plataforma modular para ejecutar pipelines Spark por capas (`raw` → `bronze` →
`silver` → `gold`) declarados en YAML. La CLI `prodi` normaliza las
configuraciones con `LayerRuntimeConfigModel` y aplica el contrato real definido
por `LayerConfig.from_dict` y `build_context`.

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

## Plantillas para Azure Databricks

El directorio `templates/azure/dbfs/` contiene una versión "copy-paste" de los
archivos necesarios para correr la cadena de capas sobre Databricks y ABFSS:

- `cfg/env.yml`: variables de entorno y credenciales `abfss` leídas desde
  Databricks (`DATABRICKS_ABFSS_KEY`).
- `datasets/toy_customers.yml`: dataset mínimo con rutas `abfss://` por capa y
  checkpoints declarados donde aplica.
- `cfg/raw.yml`, `cfg/bronze.yml`, `cfg/silver.yml`, `cfg/gold.yml`: ejemplos de
  configuración por capa usando `/dbfs/configs/datacore/...` para las rutas
  `dataset_config`/`environment_config` y `abfss://` para entradas y salidas.

Todos los sinks usan `uris.abfss` y `checkpointLocation` cuando corresponde para
eliminar dependencias en `options.path`. Las rutas de datos emplean la misma
cuenta y contenedores (`landing`, `bronze`, `silver`, `gold`) para facilitar el
cableado en entornos reales.

## Ejecución guiada

La guía detallada en [`docs/azure-databricks.md`](docs/azure-databricks.md)
explica cómo:

1. Instalar `prodi` desde el subdirectorio `lib/datacore` del repositorio usando
   `%pip install` en un notebook de Databricks.
2. Publicar las plantillas anteriores en DBFS con `dbutils.fs.put`.
3. Configurar el acceso a ABFSS con account key (y alternativa con Service
   Principal).
4. Validar cada capa con `prodi validate -c /dbfs/...` y ejecutar `prodi
   run-layer` apuntando a los YAML en DBFS.
5. Leer los Delta resultantes desde Spark para verificar la salida.

El tutorial incluye fragmentos listos para copiar/pegar que sincronizan las
rutas de configuración con los ejemplos del repositorio.

## Utilidades de la CLI

- `prodi validate -c <cfg>` valida un YAML según los modelos de Pydantic sin
  ejecutar la capa.
- `prodi run-layer <layer> -c <cfg>` normaliza, aplica overrides de calidad y
  ejecuta la capa solicitada.
- `prodi run-pipeline -p <pipeline.yml>` encadena varias capas reutilizando el
  contrato anterior.

## Estado del repositorio

- `src/datacore/` concentra los módulos de la CLI y la lógica por capa.
- `templates/azure/dbfs/` es la fuente de verdad para ejecuciones en Databricks
  + ABFSS.
- `docs/azure-databricks.md` sustituye la documentación legacy basada en
  monolitos o rutas locales.
- La política de **no Docker** se mantiene: cualquier manifest heredado debe
  eliminarse.

## Pruebas y CI

`pytest` cubre los invariantes de esquema y que los ejemplos mantienen `dry_run`
donde corresponde. Ejecuta `pytest` y `prodi validate` sobre tus configuraciones
antes de integrar cambios.
