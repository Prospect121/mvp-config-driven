# Guía de Solución de Problemas

Esta guía documenta errores comunes, soluciones y estrategias de debugging para `datacore`.

## Errores de configuración

### Error: "Configuración inválida"

**Mensaje completo**:
```
jsonschema.exceptions.ValidationError: 'type' is a required property
```

**Causa**: Falta propiedad obligatoria en YAML.

**Solución**:
1. Ejecuta validación con detalles:
```bash
prodi validate --config configs/project.yml --schema project
```

2. Revisa los campos requeridos en `datacore/config/schemas/project.schema.json`:
   - `project`, `environment`, `platform`, `datasets`
   - Cada dataset requiere: `name`, `layer`, `source`, `sink`

### Error: "incremental.merge requiere keys definidos"

**Mensaje completo**:
```
ValueError: incremental.merge requiere keys definidos
```

**Causa**: Modo `merge` sin claves primarias.

**Solución**:
```yaml
incremental:
  mode: merge
  keys: [customer_id]  # Agregar esta línea
  order_by: [updated_at DESC]
```

### Error: "source múltiple requiere merge_strategy"

**Causa**: Múltiples fuentes sin estrategia de combinación.

**Solución**:
Agrega `merge_strategy` o `transform.federate`:

```yaml
source:
  - id: src1
    type: storage
    uri: s3://data/src1/
  - id: src2
    type: storage
    uri: s3://data/src2/
merge_strategy:
  keys: [id]
  prefer: newest
  order_by: [timestamp DESC]
```

O usa federación:

```yaml
transform:
  federate:
    strategy: union
    union:
      allow_missing_columns: true
```

---

## Errores de conectividad

### Error: "No se encontró el archivo de configuración"

**Mensaje completo**:
```
FileNotFoundError: No se encontró el archivo de configuración: configs/project.yml
```

**Causa**: Ruta incorrecta o archivo no existe.

**Solución**:
1. Verifica la ruta: `ls configs/project.yml` (Linux/Mac) o `dir configs\project.yml` (Windows)
2. Usa rutas absolutas si es necesario:
```bash
prodi run --config /absolute/path/to/configs/project.yml --layer bronze
```

### Error: "JDBC connection failed"

**Mensaje completo**:
```
java.sql.SQLException: Connection refused
```

**Causa**: Base de datos no accesible o credenciales incorrectas.

**Solución**:
1. Verifica conectividad:
```bash
# PostgreSQL
psql -h localhost -U user -d database

# MySQL
mysql -h localhost -u user -p
```

2. Verifica configuración JDBC:
```yaml
source:
  type: jdbc
  url: jdbc:postgresql://localhost:5432/database  # Host y puerto correctos
  options:
    user: ${DB_USER}
    password: ${SECRET:DB_PASSWORD}
    driver: org.postgresql.Driver
```

3. Asegúrate de incluir el driver en Spark:
```yaml
spark:
  extra_conf:
    spark.jars.packages: "org.postgresql:postgresql:42.7.3"
```

### Error: "Storage access denied"

**Mensaje completo**:
```
403 Forbidden: Access denied to storage account
```

**Causa**: Credenciales inválidas o permisos insuficientes.

**Solución**:

**Azure**:
```bash
# Storage Account Key
export AZURE_STORAGE_ACCOUNT=myaccount
export AZURE_STORAGE_KEY=<key>

# O usa SAS token
export AZURE_SAS_TOKEN=<sas-token>
```

**AWS**:
```bash
export AWS_ACCESS_KEY_ID=<access-key>
export AWS_SECRET_ACCESS_KEY=<secret-key>
export AWS_REGION=us-east-1
```

**GCP**:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Verifica permisos IAM/RBAC:
- Azure: `Storage Blob Data Contributor`
- AWS: `s3:GetObject`, `s3:PutObject`
- GCP: `storage.objects.get`, `storage.objects.create`

---

## Errores de validación

### Error: "Invalid rows detected"

**Mensaje completo**:
```
INFO: Invalid rows: 245 (12.5%)
```

**Causa**: Registros no cumplen reglas de validación.

**Solución**:
1. Revisa los registros rechazados en `_rejects`:
```bash
# Leer rechazos desde sink
spark-shell
scala> spark.read.parquet("abfs://silver/customers/_rejects/").show()
```

2. Revisa métricas detalladas:
```bash
cat <sink_path>/_metrics/<run_id>.json
```

Ejemplo de salida:
```json
{
  "total_rows": 1960,
  "valid_rows": 1715,
  "invalid_rows": 245,
  "rules": {
    "expect_not_null": {"failed": 120},
    "expect_regex": {"failed": 125}
  }
}
```

3. Ajusta reglas o datos:
```yaml
validation:
  rules:
    - check: expect_not_null
      columns: [email]
      severity: warn  # Cambiar a warn si es aceptable
```

### Error: "Quarantine sink not defined"

**Causa**: Regla con `on_fail: quarantine` pero sin `quarantine_sink`.

**Solución**:
```yaml
validation:
  rules:
    - check: expect_regex
      column: email
      on_fail: quarantine
  quarantine_sink:  # Agregar esto
    type: storage
    uri: abfs://quarantine/customers/
    format: parquet
```

---

## Errores de rendimiento

### Error: "OutOfMemoryError"

**Mensaje completo**:
```
java.lang.OutOfMemoryError: Java heap space
```

**Causa**: Dataset muy grande para memoria del executor.

**Solución**:

1. Incrementa particiones:
```yaml
spark:
  shuffle_partitions: 200  # Default es 200, aumentar si es necesario
```

2. Reduce tamaño de particiones en escritura:
```yaml
sink:
  coalesce: 10  # Reducir número de archivos
  target_file_size_mb: 128  # Tamaño objetivo por archivo
```

3. Cachea con cuidado:
```yaml
cache:
  enabled: true
  storage_level: MEMORY_AND_DISK  # En vez de MEMORY_ONLY
```

4. Usa particionamiento en lectura JDBC:
```yaml
source:
  type: jdbc
  table: large_table
  partition_column: id
  lower_bound: 0
  upper_bound: 1000000
  num_partitions: 100
```

### Error: "Task too large"

**Mensaje completo**:
```
org.apache.spark.SparkException: Task not serializable
```

**Causa**: Configuración muy grande o uso de objetos no serializables.

**Solución**:
1. Simplifica transformaciones complejas
2. Evita closures con objetos grandes
3. Usa `broadcast` para DataFrames pequeños

### Error: "Shuffle fetch failed"

**Causa**: Problemas de red o executors caídos.

**Solución**:
```yaml
spark:
  extra_conf:
    spark.shuffle.service.enabled: true
    spark.dynamicAllocation.enabled: true
    spark.network.timeout: 300s
    spark.executor.heartbeatInterval: 60s
```

---

## Errores de plataforma

### Azure Databricks

**Error: "DBFS path not found"**

**Solución**:
```yaml
# Usa ABFS en vez de DBFS para mejor rendimiento
sink:
  uri: abfs://container@account.dfs.core.windows.net/path/
  # En vez de: dbfs:/mnt/path/
```

**Error: "Cluster terminated"**

**Solución**:
1. Habilita autoscaling
2. Incrementa timeout de cluster
3. Monitorea métricas en Spark UI

### AWS Glue

**Error: "GlueException: Table not found"**

**Solución**:
1. Verifica Glue Data Catalog:
```bash
aws glue get-table --database-name mydb --name mytable
```

2. Crea tabla si no existe:
```yaml
sink:
  type: storage
  format: parquet
  uri: s3://bucket/path/
  options:
    path: s3://bucket/path/
    catalogDatabase: mydb
    catalogTable: mytable
```

### GCP Dataproc

**Error: "Permission denied on GCS"**

**Solución**:
```bash
# Verifica service account
gcloud iam service-accounts list

# Otorga permisos
gsutil iam ch serviceAccount:sa@project.iam.gserviceaccount.com:objectAdmin gs://bucket
```

### Microsoft Fabric

**Error: "Lakehouse not found"**

**Solución**:
1. Verifica IDs en portal Fabric
2. Usa URIs `lakehouse://`:
```yaml
sink:
  backend: fabric
  uri: lakehouse://my-lakehouse/tables/dbo/customers
  # En vez de: https://onelake.dfs.fabric.microsoft.com/...
```

**Error: "Shortcut not created"**

**Solución**:
```yaml
source:
  type: shortcut
  name: sales_adls
  # Asegúrate de definir el shortcut en .prodi/shortcuts.yml
```

---

## Debugging avanzado

### Habilitar logs detallados

```bash
prodi run --layer bronze --config project.yml --log-level DEBUG
```

### Modo dry-run

```bash
# Ver plan sin ejecutar
prodi run --layer bronze --config project.yml --dry-run
```

Salida incluye:
- Source plan (URIs, opciones)
- Transform plan (SQL, ops)
- Validation plan (reglas)
- Sink plan (formato, particiones)

### Plan completo del proyecto

```bash
prodi plan --config project.yml > plan.json
```

Analiza `plan.json` para ver todos los datasets, dependencias y configuraciones.

### Fail-fast mode

```bash
# Detener al primer error
prodi run --layer bronze --config project.yml --fail-fast
```

### Inspeccionar checkpoints

```bash
# CDC state
cat <checkpoint_dir>/_cdc/<source_id>.json

# Streaming checkpoint
ls <checkpoint_location>/offsets/
```

### Spark UI

En ejecuciones locales, accede a `http://localhost:4040` para:
- Ver DAG de ejecución
- Analizar métricas de tasks
- Identificar stages lentos
- Revisar storage levels

---

## Recursos adicionales

- [Documentación de PySpark](https://spark.apache.org/docs/latest/api/python/)
- [Azure Databricks Troubleshooting](https://docs.microsoft.com/azure/databricks/kb/)
- [AWS Glue Troubleshooting](https://docs.aws.amazon.com/glue/latest/dg/troubleshooting.html)
- [GCP Dataproc Debugging](https://cloud.google.com/dataproc/docs/support/troubleshooting)

## Contacto y soporte

Si el problema persiste:
1. Revisa logs completos
2. Ejecuta en modo `--dry-run` para validar configuración
3. Consulta `examples/` para configuraciones de referencia
4. Abre un issue con:
   - Configuración YAML completa
   - Logs de error
   - Versión de `datacore` (`pip show datacore`)
