# Esquema de configuraciones

El proyecto ahora valida las configuraciones de ejecución y datasets mediante un
esquema declarativo basado en Pydantic. Los modelos se definen en
`src/datacore/config/schema.py` y exponen tanto objetos Pydantic como los JSON
Schema resultantes para integraciones externas.

## Configuración de ejecución por capa (`cfg/*.yml`)

Cada archivo en `cfg/` se normaliza con `migrate_layer_config` y se valida con
`LayerRuntimeConfigModel`. La estructura expone bloques consistentes:

```yaml
layer: raw
compute:
  engine: spark
io:
  source:
    dataset_config: config/datasets/example.yml
    environment_config: config/env.yml
  sink:
    database_config: config/database.yml
transform: {}
dq: {}
dry_run: true
```

Los campos `compute`, `io`, `transform` y `dq` quedan disponibles para todas las
capas y se serializan junto a `_legacy_aliases`, un mapa de alias que indica el
nuevo destino de claves planas utilizadas anteriormente (`dataset_config`,
`environment_config`, etc.).

## Configuración de datasets (`config/datasets/**/*.yml`)

Los datasets migran al formato por capas usando `migrate_dataset_config` y el
modelo `DatasetConfigModel`. Cada capa (`raw`, `bronze`, `silver`, `gold`, ...)
expone sub-secciones `compute`, `io`, `transform` y `dq`:

```yaml
layers:
  raw:
    io:
      source:
        input_format: csv
        path: s3://raw/payments/*.csv
  silver:
    transform:
      standardization: {...}
      schema: {...}
    dq:
      expectations:
        expectations_ref: config/datasets/payments/expectations.yml
  gold:
    io:
      sink:
        enabled: true
        database_config: config/database.yml
```

### Alias temporales

Para mantener compatibilidad mientras se actualiza el pipeline, se incluyen
alias en `_legacy_aliases`. Los más relevantes son:

| Clave legacy          | Ubicación actual                                      |
| --------------------- | ----------------------------------------------------- |
| `source` / `sources`  | `layers.raw.io.source`                                |
| `output.<layer>`      | `layers.<layer>.io.sink`                              |
| `standardization`     | `layers.silver.transform.standardization`            |
| `quality`             | `layers.silver.dq.expectations`                       |

El valor bajo `_legacy_aliases` es una cadena que documenta el nuevo camino
para cada clave histórica.

## Validación automática

El test `tests/test_config_schema.py` recorre todos los YAML relevantes (capas y
datasets) y confirma que el contenido respeta el esquema además de verificar la
presencia de alias críticos. Cualquier ruptura en los archivos de configuración
se detectará de forma temprana durante la ejecución de `pytest`.
