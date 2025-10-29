# Configuración

La configuración se organiza por entorno (`configs/envs/<env>`), capa (`layers/<layer>.yml`) y plataforma (`configs/platforms/<cloud>.yml`). Todos los archivos deben validar contra los esquemas JSON incluidos en `datacore/config/schemas`.

## Archivos principales
- `project.yml`: describe el proyecto, entorno, plataforma, datasets y parámetros Spark.
- `layers/<layer>.yml`: complementa con configuraciones específicas de cada capa.
- `configs/platforms/<cloud>.yml`: define URIs, rutas de checkpoints y parámetros propios del proveedor.

## Validación
El comando `prodi validate --config <ruta>` valida el YAML contra `project.schema.json` o `layer.schema.json` según corresponda. Se usa `jsonschema` con soporte para esquemas anidados.

## Variables soportadas
- `project`: nombre del proyecto.
- `environment`: `dev`, `test` o `prod`.
- `platform`: `azure`, `aws` o `gcp`.
- `datasets[]`: lista de datasets con origen, transformaciones, validaciones y sink.
- `incremental`: parámetros `mode`, `keys`, `watermark_column`.
- `streaming`: flags `enabled` y `trigger`.

## Ejemplo
Consulta los ejemplos en `configs/envs/dev` y la sección `/examples` para ver pipelines funcionales por plataforma.
