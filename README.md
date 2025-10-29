# MVP Config-Driven Pipeline

Plataforma modular para ejecutar pipelines Spark por capas (`raw` → `bronze` → `silver` → `gold`) declarados en YAML. La CLI `prodi`, instalada desde la raíz del repositorio (`pip install -e .`), normaliza las configuraciones con `LayerRuntimeConfigModel` y aplica el contrato real definido por `LayerConfig.from_dict` y `build_context`.

## Contrato de configuración

Cuando una capa se ejecuta con `dry_run: false` la configuración **debe** incluir referencias absolutas a los artefactos publicados en almacenamiento compartido:

- `dataset_config`: ruta YAML con la definición de dataset. Se resuelve desde `io.source.dataset_config` o `io.source.dataset`.
- `environment_config`: ruta YAML con la configuración de entorno. Se busca en `io.source.environment_config`, `io.source.environment`, o en la raíz como `environment_config`/`env_config`.
- `database_config`: obligatorio sólo cuando la capa `gold` publica metadatos en bases de datos. Se ubica en `io.sink.database_config` o `io.sink.database`.

Durante la construcción del contexto, `build_context` valida que las rutas apuntan a archivos reales, migra el dataset con `DatasetConfigModel` y habilita el administrador de base de datos cuando procede. Si falta alguno de los artículos anteriores la ejecución falla antes de tocar datos.

## Plantillas y CLI de planificación

Las plantillas publicadas manualmente en `/dbfs/configs/datacore/**` se retiraron. Serán sustituidas por generadores declarativos en la nueva CLI de `prodi plan/run`, que emitirá paquetes autocontenidos para cada plataforma (Databricks, Glue, Dataproc, etc.). Mientras la CLI llega a `main`, los artefactos históricos siguen disponibles bajo [`docs/legacy/`](docs/legacy/) para fines de referencia.

El flujo basado en `scripts/smoke_layers.sh` y `cfg/pipelines/example.yml` queda descontinuado. Las ejecuciones de humo y despliegues guiados se moverán a `prodi plan ...` → `prodi run ...`, evitando rutas locales (`file://...`) o copias manuales en `/dbfs`. Hasta que la nueva CLI esté lista, utiliza los `*.prod.yml` por nube como fuente de verdad y replica los pipelines desde ahí.

## Guardarraíles y CI

- La instalación editable se resuelve desde la raíz (`pip install -e .`).
- `.github/workflows/validate-configs.yml` lintéa los YAML productivos y ejecuta `prodi validate` sobre los `*.prod.yml` de cada nube.
- `tests/test_cli_layers.py` cubre invariantes de esquema y ejercita la normalización del contrato de configuración sin depender de rutas locales.

Ejecuta `pytest` y `prodi validate -c cfg/<layer>/<archivo>.yml` antes de integrar cambios para respetar el contrato de configuración.

## CI rápido (validación de YAML)

El workflow [`CI — Validate & Lint`](.github/workflows/ci-validate.yml) recorre los `cfg/*.yml` del repositorio y valida su estructura con `prodi`. No ejecuta Spark ni toca almacenamiento cloud porque forza `PRODI_FORCE_DRY_RUN=1`; solo garantiza que el contrato de configuración es consistente. Además ejecuta `ruff check` sobre `src/datacore` como retroalimentación opcional (no bloquea el merge).

Para replicar la validación localmente:

```bash
pip install -e .
pip install pyyaml typer "pydantic==2.*" fsspec adlfs gcsfs s3fs ruamel.yaml
PRODI_FORCE_DRY_RUN=1 prodi validate -c cfg/<layer>/<archivo>.yml
```

Cuando necesites pruebas integradas con Spark/Delta utiliza un workflow independiente que monte datasets locales y `sink.uris.local`; evita manejar secretos en CI.
