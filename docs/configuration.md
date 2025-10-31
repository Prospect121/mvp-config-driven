# Configuración

La configuración se organiza por entorno (`configs/envs/<env>`), capa (`layers/<layer>.yml`) y plataforma (`configs/platforms/<cloud>.yml`). Todos los archivos deben validar contra los esquemas JSON incluidos en `datacore/config/schemas`.

## Archivos principales
- `project.yml`: describe el proyecto, entorno, plataforma, datasets y parámetros Spark.
- `layers/<layer>.yml`: complementa con configuraciones específicas de cada capa.
- `configs/platforms/<cloud>.yml`: define URIs, rutas de checkpoints, secretos y parámetros propios del proveedor.

## Validación y ayudas
- El comando `prodi validate --config <ruta>` valida el YAML contra `project.schema.json` o `layer.schema.json`.
- `prodi plan --config <project.yml>` lista los datasets por capa y alerta sobre problemas rápidos (p.ej. `merge` sin claves).
- `prodi run --layer silver --config <project.yml> --dry-run` imprime el plan JSON de lectura/transformación/escritura.

## Variables y secretos
- Cualquier campo puede hacer referencia a `${ENV_VAR}` o `${SECRET:ALIAS}`. `PlatformBase.resolve_secret` resuelve primero secretos nativos de la nube y hace fallback a variables de entorno o `configs/platforms`.
- Los parámetros sensibles (tokens, cadenas de conexión) deben declararse usando estas referencias.

## Estructura de datasets
- `source`: puede ser un objeto o una lista de fuentes. Cada fuente define `type`, `uri`/`options`/`read_options`, `format`, `infer_schema`, `record_path`, `flatten`, autenticación (para `endpoint`) y paginación.
- `transform`: admite `sql`, `udf`, `ops` (operaciones declarativas) y `add_ingestion_ts` (`true` por defecto).
- `merge_strategy`: combina múltiples fuentes usando `keys`, `prefer` (`newest|left|coalesce`) y `order_by`.
- `sink`: soporta `storage`, `warehouse`, `nosql`, `kafka`, `event_hubs` con opciones específicas (particionado, `mergeSchema`, etc.).
- `incremental`: controla `mode` (`full|append|merge`), `keys`, `order_by`, `watermark_column` y banderas específicas por sink.
- `streaming`: `enabled`, `trigger`, `checkpoint`, `watermark_column`.

## Ejemplo completo (capa silver)
```yaml
project: retail360
environment: dev
platform: azure
spark:
  shuffle_partitions: 4
  extra_conf:
    spark.sql.adaptive.enabled: true
datasets:
  - name: orders_silver
    layer: silver
    source:
      - type: storage
        format: csv
        uri: abfs://landing/orders/
        infer_schema: true
        options:
          header: true
          delimiter: ";"
      - type: api_rest
        options:
          endpoint: https://api.example.com/v1/orders
          method: GET
          params:
            status: completed
          flatten: true
    merge_strategy:
      keys: [order_id]
      prefer: newest
      order_by: ["_ingestion_ts DESC"]
    transform:
      add_ingestion_ts: true
      ops:
        - trim: [customer_name]
        - uppercase: [country]
        - rename:
            country: country_code
        - cast:
            total_amount: decimal(18,2)
        - standardize_dates:
            cols: [order_date]
            format_in: "yyyy-MM-dd'T'HH:mm:ss'Z'"
            format_out: "yyyy-MM-dd"
            tz: UTC
        - deduplicate:
            keys: [order_id]
            order_by: ["order_date DESC", "_ingestion_ts DESC"]
    validation:
      rules:
        - check: expect_not_null
          columns: [order_id, order_date]
        - check: expect_unique
          columns: [order_id]
        - check: range
          column: total_amount
          min: 0
          max: 100000
        - check: regex
          column: customer_email
          pattern: "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"
      quarantine_sink:
        type: storage
        uri: abfs://silver/quarantine/orders/
    incremental:
      mode: merge
      keys: [order_id]
      order_by: ["order_date DESC", "_ingestion_ts DESC"]
      watermark_column: order_date
    sink:
      type: warehouse
      engine: synapse
      table: analytics.orders_silver
      partition_by: [order_date]
      options:
        batchsize: 10000
        truncate: false
        isolationLevel: READ_COMMITTED
        createTableOptions: "DISTRIBUTION = HASH(order_id)"
```

Consulta la carpeta `/examples` para ver variantes por plataforma y streaming.
