# Examples Directory

This directory contains working examples for various use cases and cloud platforms.

## By Platform

### Azure ([azure/](azure/))
- **[delta_merge.yaml](azure/delta_merge.yaml)**: Delta Lake merge with validation and quarantine
- **[orders_pipeline.yaml](azure/orders_pipeline.yaml)**: Complete ETL pipeline
- **[warehouse_load.yaml](azure/warehouse_load.yaml)**: Load aggregated data to Synapse Analytics

### AWS ([aws/](aws/))
- **[glue_redshift.yaml](aws/glue_redshift.yaml)**: AWS Glue with S3 and Redshift integration
- **[products_pipeline.yaml](aws/products_pipeline.yaml)**: Products ETL pipeline

### GCP ([gcp/](gcp/))
- **[bigquery_load.yaml](gcp/bigquery_load.yaml)**: Dataproc with GCS and BigQuery integration
- **[customers_pipeline.yaml](gcp/customers_pipeline.yaml)**: Customers ETL pipeline

### Microsoft Fabric ([fabric/](fabric/))
- **[bronze_shortcut.yaml](fabric/bronze_shortcut.yaml)**: Lakehouse shortcut with orchestration
- **[multicloud_passthrough.yaml](fabric/multicloud_passthrough.yaml)**: Multi-cloud data pass-through
- **[silver_federation.yaml](fabric/silver_federation.yaml)**: Multi-source federation

## By Feature

### CDC (Change Data Capture) ([cdc/](cdc/))
- **[orders_cdc.yaml](cdc/orders_cdc.yaml)**: PostgreSQL CDC with timestamp-based incremental load

### Streaming ([streaming/](streaming/))
- **[kafka_cosmos.yaml](streaming/kafka_cosmos.yaml)**: Kafka to CosmosDB streaming
- **[orders_stream.yaml](streaming/orders_stream.yaml)**: Real-time order processing

### Federation ([federation/](federation/))
- **[customers_enriched.yaml](federation/customers_enriched.yaml)**: Multi-source join (API + JDBC)

### Shortcuts ([shortcuts/](shortcuts/))
- **[sales_shortcut.yaml](shortcuts/sales_shortcut.yaml)**: Fabric Lakehouse shortcuts

### API Integration
- **[endpoint_orders.yaml](endpoint_orders.yaml)**: REST API with pagination and authentication

### JDBC
- **[jdbc_partitioned.yaml](jdbc_partitioned.yaml)**: Parallel JDBC read with partitioning

### Local Testing
- **[local_test_pipeline.yaml](local_test_pipeline.yaml)**: Complete local pipeline (raw → bronze → silver → gold) using sample CSV data

## Sample Data

The [data/](data/) directory contains sample CSV files for local testing.

## Running Examples

### Local
```bash
prodi run --layer bronze --config examples/azure/orders_pipeline.yaml --platform local
```

### With Environment Variables
```bash
export KAFKA_BOOTSTRAP=localhost:9092
export COSMOS_ENDPOINT=https://myaccount.documents.azure.com:443/
prodi run --layer bronze --config examples/streaming/orders_stream.yaml --platform azure
```

### Dry Run (Plan Only)
```bash
prodi run --layer bronze --config examples/cdc/orders_cdc.yaml --dry-run
```

## Validating Examples

All examples can be validated against schemas:

```bash
prodi validate --config examples/azure/delta_merge.yaml
```

## Contributing

When adding new examples:
1. Use meaningful project names
2. Include comments explaining key configurations
3. Use environment variables for secrets (${SECRET:KEY_NAME})
4. Test against the schema before committing
5. Update this README with a brief description
