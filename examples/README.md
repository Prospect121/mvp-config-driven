# Ejemplos por plataforma

Cada carpeta contiene pipelines de referencia raw→gold para Azure, AWS, GCP y escenarios streaming.

## Datos
- `data/customers.csv`: dataset base utilizado en los ejemplos dev.

## Pipelines destacados
- `azure/orders_pipeline.yaml`: ingesta multi-fuente (ABFS + API REST) → parquet silver → Synapse gold.
- `aws/products_pipeline.yaml`: normalización JSON → parquet bronze → Redshift merge.
- `gcp/customers_pipeline.yaml`: unión Parquet + GraphQL → parquet silver → BigQuery gold.
- `endpoint_orders.yaml`: ingesta REST paginada con autenticación bearer hacia almacenamiento S3.
- `jdbc_partitioned.yaml`: lectura JDBC paralelizada con pushdown hacia parquet raw.
- `streaming/kafka_cosmos.yaml`: streaming Kafka → CosmosDB con watermark y checkpoint dedicado.
- `streaming/orders_stream.yaml`: pipeline base para lecturas Kafka particionadas con watermark declarativo.

## Ejecución local (Spark standalone)
```bash
prodi validate --config examples/azure/orders_pipeline.yaml
prodi plan --config examples/aws/products_pipeline.yaml
prodi run --layer silver --config examples/gcp/customers_pipeline.yaml --dry-run
```

Para ejecuciones completas define las plataformas (`configs/platforms/*.yml`) y secretos necesarios (`${SECRET:...}`).
