# Casos de Uso

Esta guía documenta casos de uso reales con ejemplos prácticos y configuraciones completas.

## Caso 1: Ingestión de CSV a Delta Lake

**Escenario**: Ingerir archivos CSV desde almacenamiento cloud a Delta Lake para análisis.

**Requisitos previos**:

- Archivos CSV en S3/ABFS/GCS
- Permisos de lectura/escritura en storage

**Configuración**:

```yaml
project: csv-to-delta
environment: prod
platform: azure
datasets:
  - name: sales_bronze
    layer: bronze
    source:
      type: storage
      backend: abfs
      format: csv
      uri: abfs://landing/sales/sales_202411.csv
      options:
        header: true
        inferSchema: true
        delimiter: ","
    transform:
      add_ingestion_ts: true
    validation:
      rules:
        - check: expect_not_null
          columns: [order_id, customer_id, total_amount]
          severity: error
    sink:
      type: storage
      backend: abfs
      format: delta
      uri: abfs://bronze/sales/
      mode: append
      partition_by: [year, month]
```

**Ejecutar**:

```bash
prodi run --layer bronze --config configs/sales_bronze.yml --platform azure
```

**Resultado esperado**: Archivos Delta particionados por año/mes en `abfs://bronze/sales/`.

> 💡 **Ejemplo completo**: Ver [examples/azure/delta_merge.yaml](../examples/azure/delta_merge.yaml) para un ejemplo con validación y cuarentena.

---

## Caso 2: Federación de múltiples fuentes (JDBC)

**Escenario**: Combinar datos de ERP (PostgreSQL) y CRM (MySQL) mediante un JOIN.

**Requisitos previos**:

- Acceso JDBC a ambas bases de datos
- Variables de entorno `ERP_JDBC_URL`, `CRM_JDBC_URL`

**Configuración**:

```yaml
project: customer-federation
environment: dev
platform: local
spark:
  extra_conf:
    spark.jars.packages: "org.postgresql:postgresql:42.7.3,mysql:mysql-connector-java:8.0.33"
datasets:
  - name: customers_federated
    layer: raw
    source:
      - id: erp
        type: jdbc
        url: ${ERP_JDBC_URL}
        options:
          driver: org.postgresql.Driver
        table: public.customers
      - id: crm
        type: jdbc
        url: ${CRM_JDBC_URL}
        options:
          driver: com.mysql.cj.jdbc.Driver
        table: crm.customer_segments
    transform:
      federate:
        strategy: join
        join:
          left: erp
          right: crm
          on:
            - { left: email, right: email }
          join_type: left
          select:
            - { expr: "erp.customer_id", as: "customer_id" }
            - { expr: "erp.email", as: "email" }
            - { expr: "erp.country", as: "country" }
            - { expr: "coalesce(crm.segment, 'UNCLASSIFIED')", as: "segment" }
      ops:
        - trim: [email]
        - lowercase: [email]
    validation:
      rules:
        - check: expect_not_null
          columns: [customer_id, email]
    sink:
      type: storage
      backend: local
      format: parquet
      uri: /tmp/lake/raw/customers_federated
      mode: overwrite
```

**Ejecutar**:

```bash
export ERP_JDBC_URL="jdbc:postgresql://localhost:5432/erp"
export CRM_JDBC_URL="jdbc:mysql://localhost:3306/crm"
prodi run --layer raw --config configs/customer_federation.yml
```

**Resultado esperado**: Dataset unificado en `/tmp/lake/raw/customers_federated` con columnas de ambas fuentes.

---

## Caso 3: Carga incremental con CDC (Change Data Capture)

**Escenario**: Leer solo registros nuevos/actualizados de SQL Server usando timestamp.

**Requisitos previos**:

- Tabla con columna `updated_at` (timestamp)
- Permisos de lectura en SQL Server

**Configuración**:

```yaml
project: orders-cdc
environment: prod
platform: azure
datasets:
  - name: orders_incremental
    layer: bronze
    source:
      type: jdbc
      url: ${SQL_SERVER_URL}
      table: dbo.orders
      options:
        driver: com.microsoft.sqlserver.jdbc.SQLServerDriver
      cdc:
        mode: timestamp
        watermark_column: updated_at
    incremental:
      mode: merge
      keys: [order_id]
      order_by: [updated_at DESC]
    sink:
      type: storage
      backend: abfs
      format: delta
      uri: abfs://bronze/orders/
      mode: append
```

**Primera ejecución**:

```bash
prodi run --layer bronze --config configs/orders_cdc.yml
```

Cargará todos los registros y almacenará el watermark (ej. `2025-11-24 12:00:00`).

**Segunda ejecución**:

```bash
prodi run --layer bronze --config configs/orders_cdc.yml
```

Solo cargará registros con `updated_at > '2025-11-24 12:00:00'`.

**Resultado esperado**: Carga incremental eficiente sin duplicados.

> 💡 **Ejemplo completo**: Ver [examples/cdc/orders_cdc.yaml](../examples/cdc/orders_cdc.yaml)

---

## Caso 4: Integración con API REST

**Escenario**: Ingestar pedidos desde API REST paginada y escribir a Delta.

**Requisitos previos**:

- API accesible con autenticación bearer
- Token almacenado en `API_TOKEN` (env var)

**Configuración**:

```yaml
project: api-integration
environment: prod
platform: aws
datasets:
  - name: orders_from_api
    layer: bronze
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
        cursor_path: $.pagination.next_cursor
      record_path: data
      flatten: true
    transform:
      ops:
        - rename:
            created_at: order_date
        - cast:
            total: decimal(18,2)
    validation:
      rules:
        - check: expect_not_null
          columns: [order_id, customer_id]
        - check: expect_range
          column: total
          params:
            min: 0
            max: 1000000
    sink:
      type: storage
      backend: s3
      format: delta
      uri: s3://bronze/orders_api/
      mode: append
```

**Ejecutar**:

```bash
export API_TOKEN="your-bearer-token"
prodi run --layer bronze --config configs/api_orders.yml --platform aws
```

**Resultado esperado**: Todos los registros paginados escritos en Delta Lake.

---

## Caso 5: Streaming con Kafka

**Escenario**: Procesar eventos en tiempo real desde Kafka y escribir a Event Hubs.

**Requisitos previos**:

- Kafka cluster accesible
- Event Hubs connection string

**Configuración**:

```yaml
project: streaming-pipeline
environment: prod
platform: fabric
datasets:
  - name: events_streaming
    layer: bronze
    source:
      type: kafka
      options:
        kafka.bootstrap.servers: kafka.example.com:9092
        subscribe: events
        startingOffsets: latest
      payload_format: json
    streaming:
      enabled: true
      watermark:
        column: event_timestamp
        delay_threshold: 10 minutes
      trigger: 30 seconds
      checkpoint_location: abfs://checkpoints/events
    transform:
      ops:
        - flatten_json:
            columns: [payload]
    validation:
      rules:
        - check: expect_not_null
          columns: [event_id, event_timestamp]
    sink:
      type: event_hubs
      options:
        eventHubs.connectionString: ${SECRET:EVENT_HUBS_CONN}
        topic: enriched_events
```

**Ejecutar**:

```bash
prodi run --layer bronze --config configs/streaming.yml --platform fabric
```

**Resultado esperado**: Stream continuo procesando eventos con watermark de 10 minutos.

> 💡 **Ejemplo completo**: Ver [examples/streaming/orders_stream.yaml](../examples/streaming/orders_stream.yaml)

---

## Caso 6: Validación con cuarentena

**Escenario**: Validar datos de clientes y enviar registros inválidos a cuarentena.

**Requisitos previos**:

- Source con datos potencialmente inválidos

**Configuración**:

```yaml
project: data-quality
environment: prod
platform: azure
datasets:
  - name: customers_validated
    layer: silver
    source:
      type: storage
      backend: abfs
      format: parquet
      uri: abfs://bronze/customers/
    validation:
      rules:
        - check: expect_not_null
          columns: [customer_id, email]
          severity: error
        - check: expect_regex
          column: email
          params:
            pattern: "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"
          severity: error
          on_fail: quarantine
        - check: expect_range
          column: age
          params:
            min: 18
            max: 120
          severity: warn
          on_fail: quarantine
      quarantine_sink:
        type: storage
        backend: abfs
        format: parquet
        uri: abfs://quarantine/customers/
        mode: append
    sink:
      type: storage
      backend: abfs
      format: delta
      uri: abfs://silver/customers/
      mode: overwrite
```

**Ejecutar**:

```bash
prodi run --layer silver --config configs/customers_validation.yml
```

**Resultado esperado**:

- Registros válidos → `abfs://silver/customers/`
- Registros inválidos → `abfs://quarantine/customers/`
- Métricas JSON con conteo de rechazos

> 💡 **Ejemplo completo**: Ver [examples/azure/delta_merge.yaml](../examples/azure/delta_merge.yaml)

---

## Caso 7: Escritura a Data Warehouse (Synapse)

**Escenario**: Cargar datos transformados a Azure Synapse Analytics.

**Requisitos previos**:

- Synapse workspace configurado
- Credenciales JDBC

**Configuración**:

```yaml
project: synapse-load
environment: prod
platform: azure
datasets:
  - name: sales_gold
    layer: gold
    source:
      type: storage
      backend: abfs
      format: delta
      uri: abfs://silver/sales/
    transform:
      sql:
        - >
          SELECT
            product_category,
            DATE_TRUNC('month', order_date) AS month,
            SUM(total_amount) AS revenue,
            COUNT(DISTINCT customer_id) AS customers
          FROM _src
          GROUP BY product_category, DATE_TRUNC('month', order_date)
    sink:
      type: warehouse
      engine: synapse
      table: analytics.sales_monthly
      mode: overwrite
      batch_size: 10000
      isolation_level: READ_COMMITTED
      create_table_options: "DISTRIBUTION = HASH(product_category)"
```

**Ejecutar**:

```bash
prodi run --layer gold --config configs/synapse_sales.yml
```

**Resultado esperado**: Tabla `analytics.sales_monthly` creada/actualizada en Synapse.

> 💡 **Ejemplo completo**: Ver [examples/azure/warehouse_load.yaml](../examples/azure/warehouse_load.yaml)

---

## Errores comunes

### Error: "Table not found"

- **Causa**: Tabla JDBC no existe o nombre incorrecto
- **Solución**: Verificar nombre de tabla y esquema

### Error: "Invalid JSON"

- **Causa**: Payload de API/Kafka no es JSON válido
- **Solución**: Verificar `payload_format` y estructura de respuesta

### Error: "Watermark column not found"

- **Causa**: Columna de watermark no existe en streaming
- **Solución**: Asegurar que la columna existe en el schema de entrada

### Error: "Merge without keys"

- **Causa**: `incremental.mode: merge` sin definir `keys`
- **Solución**: Agregar `keys: [primary_key]` en sección `incremental`

## Próximos pasos

- Explora más ejemplos en [examples/](../examples/) - ver [examples/README.md](../examples/README.md)
- Lee [troubleshooting.md](troubleshooting.md) para soluciones detalladas
- Consulta [connectors.md](connectors.md) para opciones específicas de cada conector
- Revisa [federation/customers_enriched.yaml](../examples/federation/customers_enriched.yaml) para federación multi-fuente
- Revisa [endpoint_orders.yaml](../examples/endpoint_orders.yaml) para integración con APIs REST
