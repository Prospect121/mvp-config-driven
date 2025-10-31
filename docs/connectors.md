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
    mergeSchema: true
sink:
  type: storage
  format: parquet
  uri: abfs://silver/sales/
  partition_by: [year, month]
  options:
    compression: snappy
```

## APIs
- **REST** (`datacore.connectors.api.rest`): `fetch_pages` soporta paginación, backoff, cabeceras dinámicas y flatten opcional.
- **GraphQL** (`datacore.connectors.api.graphql`): `execute_query` permite queries parametrizadas con `variables`.

```yaml
source:
  type: api_rest
  options:
    endpoint: https://api.example.com/orders
    method: GET
    params:
      status: completed
    flatten: true
```

## Bases de datos y warehouses
- **JDBC** (`datacore.connectors.db.jdbc`): Postgres, SQL Server/Synapse, MySQL, Redshift. Soporta `pushdown`, lectura por particiones (`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`) y upserts en modo `merge`.
- **Warehouse** (`datacore.io.writers`): escritura JDBC con `batchsize`, `isolationLevel`, `truncate` seguro y `createTableOptions`.
- **BigQuery** (`datacore.connectors.db.bigquery`): escritura batch con `temporaryGcsBucket` e `intermediateFormat`.

```yaml
sink:
  type: warehouse
  engine: postgres
  table: analytics.orders
  options:
    batchsize: 5000
    truncate: false
    isolationLevel: READ_COMMITTED
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
Los lectores y escritores streaming utilizan `readStream`/`writeStream`. El payload puede ser JSON o CSV y se especifica mediante `options.format`. Se soportan `watermark_column`, `trigger`, `checkpointLocation` y `topic` / `eventHubs.connectionString`.

```yaml
source:
  type: kafka
  options:
    subscribe: orders
    startingOffsets: earliest
    format: json
    watermark_column: event_time
sink:
  type: event_hubs
  options:
    eventHubs.connectionString: ${SECRET:EH_CONN}
    topic: orders_enriched
    checkpointLocation: abfs://checkpoints/orders
```
