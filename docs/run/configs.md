# Esquema de configuraciones

El pipeline valida todas las configuraciones YAML con modelos de Pydantic
definidos en `src/datacore/config/schema.py`. Cada archivo que vive en `cfg/`
se convierte en un `LayerRuntimeConfigModel` y, cuando aplica, los datasets bajo
`config/datasets/` usan `DatasetConfigModel`. La validación ocurre tanto en
`prodi run-layer`/`run-pipeline` como en CI (`pytest` + `prodi validate`).

## Capas de ejecución (`cfg/<layer>/*.yml`)

Las capas comparten la misma estructura de alto nivel con bloques
`compute`/`io`/`transform`/`dq`. Un ejemplo de ingesta HTTP incremental:

```yaml
layer: raw
compute:
  engine: spark
  spark:
    app_name: "raw_fin_transactions_http"
io:
  source:
    type: http
    url: "https://api.finbank.com/v1/transactions"
    auth:
      type: bearer_env
      env: FINBANK_TOKEN
    pagination:
      strategy: param_increment
      param: page
      max_pages: 2000
    rate_limit:
      requests_per_minute: 60
    incremental:
      watermark:
        field: updated_at
        state_id: raw_fin_transactions_http
        default: "1970-01-01T00:00:00Z"
  sink:
    type: files
    format: parquet
    path: "s3://datalake/raw/finance/transactions/date={{ds}}/"
transform:
  pre:
    - rename:
        from: txn_id
        to: transaction_id
dq:
  fail_on_error: true
  expectations:
    - type: not_null
      column: transaction_id
    - type: valid_values
      column: currency
      values: ["USD", "EUR", "COP"]
```

- `compute.spark.app_name` y `compute.spark.options` se transfieren a la sesión
  de Spark.
- `io.source` y `io.sink` son uniones tipadas: `type: http|jdbc|files` decide
  qué campos son obligatorios. Las secciones aceptan `options` y overrides por
  filesystem (`filesystem.storage_options`, `filesystem.reader_options`, ...).
- `incremental.watermark` resuelve el valor a partir del estado persistente y lo
  reescribe al finalizar la capa.
- `dq.expectations` soporta `severity: warn|error`. El reporte JSON/Markdown se
  genera cuando se declara `dq.report.path`.

### Autenticación declarativa

| Bloque               | Tipo (`type`)               | Descripción                                               |
| -------------------- | -------------------------- | --------------------------------------------------------- |
| `io.source.auth`     | `bearer_env`                | Lee `env` y construye `Authorization: Bearer <token>`     |
|                      | `api_key_header`            | Usa `header` + `env` para inyectar llaves en HTTP          |
|                      | `basic_env`                 | Resuelve `username_env`/`password_env` para Basic Auth     |
|                      | `oauth2_client_credentials` | Ejecuta client-credentials (`token_url`, scopes opcionales) |
| `io.source.auth`     | `managed_identity` (JDBC)   | Habilita `azure.identity.auth.type=ManagedIdentity`        |
|                      | `basic_env` (JDBC)          | Lee usuario/clave desde variables sin exponer secretos     |

No dupliques la cabecera `Authorization`: cuando se declara un bloque `auth`
no es necesario (ni seguro) inyectar la cabecera manualmente en `io.source.headers`.

### Particionamiento JDBC

Cuando un conector JDBC declara `partitioning.column` pero omite
`lower_bound`/`upper_bound`, el runtime calcula automáticamente ambos valores
ejecutando una consulta segura con `MIN()`/`MAX()` sobre la subconsulta base.
Esta heurística (habilitada por defecto) permite mantener los YAML limpios sin
perder el particionado paralelo. Se puede desactivar fijando
`partitioning.discover: false` o declarando manualmente los límites.

### Herencia (`extends`)

Los overlays definen `extends: "../base.yml"` para reutilizar una configuración
existente. Durante la carga se hace un merge profundo y se reemplazan sólo las
secciones presentes en el overlay. Esto se utiliza para los casos de uso de
fraude y cobranza (`cfg/use_cases/...`).

Cuando un overlay redefine SQL debe leer desde `__BASE__` en lugar de
`__INPUT__`. `__BASE__` representa el resultado final de la configuración
extendida y evita ejecutar dos veces la cláusula `FROM`. El placeholder
`__INPUT__` queda reservado para los archivos base que consumen la entrada de la
capa previa.

### Pipelines declarativos

Los pipelines (`cfg/pipelines/*.yml`) encadenan capas respetando la separación
por dominios. Se pueden parametrizar con `--vars` para reutilizar un mismo
pipeline con fuentes distintas. Ejemplo:

```bash
prodi run-pipeline -p cfg/pipelines/finance_transactions.yml --vars RAW_SOURCE=http
prodi run-pipeline -p cfg/pipelines/finance_transactions.yml --vars RAW_SOURCE=jdbc
```

El pipeline de finanzas `cfg/pipelines/finance_transactions.yml` selecciona la
fuente RAW (`http` o `jdbc`) y continúa con las capas bronze, silver y gold
(`cfg/finance/gold/kpis.yml`). Si no se declara `RAW_SOURCE`, se usa `http` y el
watermark inicial se puede inyectar con `RAW_WM_TS` (por defecto,
`1970-01-01T00:00:00Z`).

## Configuraciones de dataset (`config/datasets/**/*.yml`)

Los datasets se normalizan con `migrate_dataset_config` y luego pasan por
`DatasetConfigModel`. Cada capa (`raw`, `bronze`, `silver`, `gold`, ...) tiene
sus propios bloques `compute`, `io`, `transform` y `dq`. El modelo mantiene un
mapa `_legacy_aliases` con el destino final de claves planas antiguas.

```yaml
layers:
  raw:
    io:
      source:
        type: http
        url: https://api.example.com/items
        incremental:
          watermark:
            field: updated_at
            state_id: example_items
  bronze:
    transform:
      sql: ["SELECT * FROM __INPUT__"]
  gold:
    io:
      sink:
        type: files
        path: s3://datalake/gold/items/
```

## Validación automática

- `prodi validate -c cfg/finance/raw/transactions_http.yml` valida un YAML sin
  ejecutarlo.
- `pytest` ejecuta `tests/test_config_schema.py`, que recorre todos los archivos
  bajo `cfg/` y `config/datasets/` para garantizar compatibilidad.
- El estado incremental se persiste bajo `data/_state/<state_id>.json` y se
  utiliza automáticamente en las re-ejecuciones.
