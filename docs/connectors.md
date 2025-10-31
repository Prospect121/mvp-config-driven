# Conectores disponibles

Los conectores implementan puertos específicos para cada tipo de fuente o destino y se instancian automáticamente desde `datacore.io` según la configuración del dataset.

## Storage
- **S3** (`datacore.connectors.storage.s3`)
- **Azure Data Lake Storage Gen2 / ABFS** (`datacore.connectors.storage.abfs`)
- **Google Cloud Storage** (`datacore.connectors.storage.gcs`)
- **Local/MinIO** (`datacore.connectors.storage.local`)

Formatos soportados: `csv`, `json` (multiLine), `parquet`, `avro`, `orc`. Las opciones principales incluyen `header`, `inferSchema`, `delimiter`, `compression`, `mergeSchema`, `partitionBy` y `pathGlobFilter`. Todos los conectores exponen `read` y `write` batch/streaming.

```yaml
source:
  type: storage
  format: parquet
  uri: s3://landing/sales/
  options:
  merge_schema: true
sink:
  type: storage
  format: parquet
  uri: abfs://silver/sales/
  partition_by: [year, month]
  compression: snappy
```

## APIs y endpoints HTTP
- **REST** (`datacore.connectors.api.rest`): `fetch_pages` soporta paginación, backoff, cabeceras dinámicas y flatten opcional.
- **GraphQL** (`datacore.connectors.api.graphql`): `execute_query` permite queries parametrizadas con `variables`.
- **Endpoint genérico** (`datacore.connectors.http`): autenticación `bearer`/`basic`, reintentos exponenciales y paginación por página, cursor o cabecera `Link`. El lector normaliza JSON a columnas y permite `flatten_depth` configurable.

```yaml
source:
  type: endpoint
  url: https://api.example.com/v1/orders
  method: GET
  auth:
    type: bearer
    token_env: API_TOKEN
  pagination:
    strategy: cursor
    param: cursor
    cursor_path: $.next
  record_path: items
  flatten: true
```

## Bases de datos y warehouses
- **JDBC** (`datacore.connectors.db.jdbc`): Postgres, SQL Server/Synapse, MySQL, Redshift. Soporta `pushdown`, lectura por particiones (`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, `fetchsize`) y upserts en modo `merge`.
- **Warehouse** (`datacore.io.writers`): escritura JDBC con `batch_size`, `isolation_level`, `truncate_safe` y `create_table_options`.
- **BigQuery** (`datacore.io.writers`): escritura batch con `temporary_gcs_bucket` e `intermediate_format`.

```yaml
sink:
  type: warehouse
  engine: postgres
  table: analytics.orders
  batch_size: 5000
  isolation_level: READ_COMMITTED
```

## NoSQL
- **CosmosDB** (`sink.engine: cosmosdb`): Spark connector oficial con soporte para `checkpointLocation` y `partitionKey`.
- **DynamoDB** (`sink.engine: dynamodb`): escritor híbrido (Spark + boto3) con soporte para catálogos Glue.

```yaml
sink:
  type: nosql
  engine: cosmosdb
  options:
    endpoint: ${SECRET:COSMOS_URI}
    masterkey: ${SECRET:COSMOS_KEY}
    database: sales
    collection: orders
```

## Streaming (Kafka / Event Hubs)
Los lectores y escritores streaming utilizan `readStream`/`writeStream`. El payload puede ser JSON o CSV y se especifica mediante `payload_format`. Se soportan `watermark` (`{column, delay_threshold}`), `trigger`, `checkpoint_location` y `topic` / `eventHubs.connectionString`.

```yaml
source:
  type: kafka
  options:
    subscribe: orders
    startingOffsets: earliest
  payload_format: json
streaming:
  enabled: true
  watermark:
    column: event_time
    delay_threshold: "10 minutes"
sink:
  type: event_hubs
  options:
    eventHubs.connectionString: ${SECRET:EH_CONN}
    topic: orders_enriched
  checkpoint_location: abfs://checkpoints/orders
```
