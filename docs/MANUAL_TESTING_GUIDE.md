# Guía de Pruebas Manuales - Pipeline Config-Driven

## Descripción General

Este documento proporciona instrucciones paso a paso para probar manualmente el pipeline config-driven completo, verificar los resultados esperados y conectarse a la base de datos PostgreSQL desde DBeaver.

## Tabla de Contenidos

1. [Prerrequisitos](#prerrequisitos)
2. [Configuración del Entorno](#configuración-del-entorno)
3. [Prueba del Pipeline Completo](#prueba-del-pipeline-completo)
4. [Verificación de Resultados](#verificación-de-resultados)
5. [Conexión con DBeaver](#conexión-con-dbeaver)
6. [Solución de Problemas](#solución-de-problemas)

---

## Prerrequisitos

### Software Requerido
- Docker Desktop instalado y ejecutándose
- DBeaver Community Edition (opcional, para visualización de datos)
- Terminal/PowerShell con acceso a comandos Docker

### Verificación de Prerrequisitos
```bash
# Verificar que Docker está funcionando
docker --version
docker-compose --version

# Verificar que los contenedores no están ejecutándose
docker ps
```

---

## Configuración del Entorno

### 1. Iniciar los Servicios Docker

```bash
# Desde el directorio raíz del proyecto
cd c:\Users\erick\Documents\work\Prodigio\a_ruta_pass\mvp-config-driven

# Iniciar todos los servicios
docker-compose up -d

# Verificar que los contenedores están ejecutándose
docker ps
```

**Contenedores esperados:**
- `mvp-config-driven-runner-1` (Spark/Python)
- `mvp-config-driven-postgres-1` (PostgreSQL)
- `mvp-config-driven-minio-1` (MinIO)
- `mvp-config-driven-spark-master-1` (Spark Master)
- `mvp-config-driven-spark-worker-1` (Spark Worker)

### 2. Verificar Conectividad de la Base de Datos

```bash
# Probar conexión a PostgreSQL
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "SELECT version();"
```

**Resultado esperado:** Información de la versión de PostgreSQL

---

## Prueba del Pipeline Completo

### 1. Preparar Datos de Prueba

Los datos de prueba ya están incluidos en el proyecto:
- **Archivo:** `data/raw/payments/sample.csv`
- **Configuración:** `config/datasets/finanzas/payments_v1/dataset.yml`
- **Base de datos:** `config/database.yml`
- **Entorno:** `config/env.yml`

### 2. Limpiar Datos Anteriores (Opcional)

```bash
# Limpiar tabla de pruebas anteriores
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "DROP TABLE IF EXISTS gold.payments_v1;"

# Limpiar archivos Silver anteriores (en MinIO/S3)
# Los archivos se sobrescriben automáticamente con el modo overwrite_dynamic
```

### 3. Ejecutar el Pipeline

```bash
# Ejecutar el pipeline completo con Gold layer
docker exec mvp-config-driven-runner-1 python /mvp/pipelines/spark_job_with_db.py /mvp/config/datasets/finanzas/payments_v1/dataset.yml /mvp/config/env.yml /mvp/config/database.yml development
```

**Parámetros del comando:**
- `dataset.yml`: Configuración del dataset (fuente, transformaciones, calidad, esquema)
- `env.yml`: Configuración del entorno (timezone, configuraciones globales)
- `database.yml`: Configuración de la base de datos PostgreSQL
- `development`: Entorno específico a usar (default, development, production)

### 4. Interpretar la Salida del Pipeline

**Salida esperada exitosa:**
```
[source] Loaded rows from s3a://raw/payments/2025/09/26/*.csv
[quality] stats: {'total': 5, 'passed': 5, 'quarantined': 0, 'dropped': 0}
[silver] Final row count: 5
[silver] Successfully wrote to s3a://silver/payments_v1/
[gold] Database configuration loaded for environment: development
[gold] Starting Gold layer processing for dataset: payments_v1
[gold] Creating/updating table 'payments_v1' from schema 'config/datasets/finanzas/payments_v1/schema.json'
[gold] Filtered DataFrame to schema columns: ['payment_id', 'customer_id', 'amounts', 'currency', 'payment_date', 'updated_at']
[gold] Successfully wrote data to table 'payments_v1' in append mode
[gold] Pipeline completed successfully!
```

---

## Verificación de Resultados

### 1. Verificar Capa Silver (Archivos Parquet en S3/MinIO)

```bash
# Los archivos se almacenan en MinIO/S3, verificar a través de logs del pipeline
# o usando herramientas de MinIO client si están configuradas
```

**Estructura esperada en S3:**
```
s3a://silver/payments_v1/
├── year=2025/
│   └── month=1/
│       └── part-00000-*.parquet
```

### 2. Verificar Capa Gold (Base de Datos PostgreSQL)

#### Verificar esquemas disponibles:
```bash
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "\dn"
```

#### Verificar que la tabla fue creada en el esquema gold:
```bash
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "\dt gold.*"
```

#### Verificar esquema de la tabla:
```bash
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "\d gold.payments_v1"
```

#### Verificar datos insertados:
```bash
# Contar registros
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "SELECT COUNT(*) as total_rows FROM gold.payments_v1;"

# Ver todos los datos
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "SELECT payment_id, customer_id, amounts, currency, payment_date FROM gold.payments_v1 ORDER BY payment_id;"
```

**Datos esperados (5 registros):**
```
 payment_id | customer_id | amounts | currency |    payment_date    
------------+-------------+---------+----------+--------------------
 p-001      | cust_001    | 1500.50 | USD      | 2025-01-15 10:30:00
 p-002      | cust_002    | 2750.75 | EUR      | 2025-01-15 11:45:00
 p-003      | cust_003    |  999.99 | CLP      | 2025-01-15 14:20:00
 p-004      | cust_001    | 3200.00 | USD      | 2025-01-15 16:10:00
 p-005      | cust_004    |  850.25 | EUR      | 2025-01-15 17:30:00
```

### 3. Verificar Calidad de Datos

#### Verificar constraints de la tabla:
```bash
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "SELECT conname, contype, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'gold.payments_v1'::regclass;"
```

#### Verificar que no hay datos en cuarentena:
```bash
# Los datos de cuarentena se almacenan en S3/MinIO según la configuración
# Verificar a través de logs del pipeline o herramientas de MinIO client
# Ruta de cuarentena configurada: s3a://raw/quarantine/payments_v1/
```

#### Verificar metadatos de ejecución:
```bash
# Ver historial de ejecuciones del pipeline
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "SELECT * FROM metadata.pipeline_executions ORDER BY started_at DESC LIMIT 5;"

# Ver versiones de datasets
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "SELECT * FROM metadata.dataset_versions ORDER BY created_at DESC LIMIT 5;"
```

---

## Conexión con DBeaver

### 1. Configuración de Conexión

1. **Abrir DBeaver** y crear una nueva conexión
2. **Seleccionar PostgreSQL** como tipo de base de datos
3. **Configurar los parámetros de conexión:**

| Campo | Valor |
|-------|-------|
| **Host** | `localhost` |
| **Puerto** | `5432` |
| **Base de datos** | `data_warehouse` |
| **Usuario** | `postgres` |
| **Contraseña** | `postgres123` |

### 2. Configuración Avanzada

En la pestaña **"Driver properties"** o **"Advanced"**:
- **SSL Mode:** `disable` (para desarrollo local)
- **Connect timeout:** `30`

### 3. Probar Conexión

1. Hacer clic en **"Test Connection"**
2. Si es la primera vez, DBeaver descargará automáticamente el driver de PostgreSQL
3. **Resultado esperado:** "Connected" con información de la versión

### 4. Explorar Datos

Una vez conectado:

1. **Navegar a:** `data_warehouse` → `Schemas`
2. **Esquemas disponibles:**
   - `gold`: Contiene las tablas de datos procesados
   - `metadata`: Contiene tablas de metadatos del pipeline
   - `public`: Esquema por defecto (puede estar vacío)
3. **Encontrar tabla:** `gold` → `Tables` → `payments_v1`
4. **Ver datos:** Clic derecho → "View Data"
5. **Ver estructura:** Clic derecho → "View DDL"

### 5. Consultas de Ejemplo

```sql
-- Contar registros por moneda
SELECT currency, COUNT(*) as count, SUM(amounts) as total_amount
FROM gold.payments_v1 
GROUP BY currency 
ORDER BY count DESC;

-- Pagos por cliente
SELECT customer_id, COUNT(*) as payment_count, SUM(amounts) as total_spent
FROM gold.payments_v1 
GROUP BY customer_id 
ORDER BY total_spent DESC;

-- Pagos por fecha
SELECT DATE(payment_date) as payment_day, COUNT(*) as payments
FROM gold.payments_v1 
GROUP BY DATE(payment_date) 
ORDER BY payment_day;

-- Verificar metadatos del pipeline
SELECT dataset_name, status, records_processed, started_at, completed_at
FROM metadata.pipeline_executions 
WHERE dataset_name = 'payments_v1'
ORDER BY started_at DESC;

-- Verificar versiones de esquemas
SELECT dataset_name, version, schema_hash, created_at
FROM metadata.dataset_versions 
WHERE dataset_name = 'payments_v1'
ORDER BY created_at DESC;
```

---

## Solución de Problemas

### Problema: Contenedores no inician

**Síntomas:**
```bash
docker-compose up -d
# Error: port already in use
```

**Solución:**
```bash
# Detener contenedores existentes
docker-compose down

# Verificar puertos en uso
netstat -an | findstr :5432
netstat -an | findstr :9000

# Reiniciar servicios
docker-compose up -d
```

### Problema: Error de conexión a PostgreSQL

**Síntomas:**
```
Failed to write DataFrame to table: Connection refused
```

**Solución:**
```bash
# Verificar estado del contenedor PostgreSQL
docker logs mvp-config-driven-postgres-1

# Reiniciar contenedor si es necesario
docker-compose restart postgres

# Esperar a que PostgreSQL esté listo
docker exec mvp-config-driven-postgres-1 pg_isready -U postgres
```

### Problema: Error "Column year not found in schema"

**Síntomas:**
```
Column year not found in schema
```

**Solución:**
Este error indica que hay una versión anterior del código. Verificar que se está usando la versión actualizada de `spark_job_with_db.py` que incluye el filtrado de columnas.

### Problema: Error "Duplicate key value"

**Síntomas:**
```
ERROR: duplicate key value violates unique constraint "gold.payments_v1_pkey"
```

**Solución:**
```bash
# Limpiar datos anteriores
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "DELETE FROM gold.payments_v1;"

# O eliminar la tabla completamente
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "DROP TABLE IF EXISTS gold.payments_v1;"
```

### Problema: Error "Schema 'gold' does not exist"

**Síntomas:**
```
ERROR: schema "gold" does not exist
```

**Solución:**
```bash
# Verificar que el script de inicialización se ejecutó correctamente
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "CREATE SCHEMA IF NOT EXISTS gold;"
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "CREATE SCHEMA IF NOT EXISTS metadata;"

# O reiniciar el contenedor para que se ejecute el script de inicialización
docker-compose restart postgres
```

### Problema: Error de configuración de tabla_settings

**Síntomas:**
```
KeyError: 'table_settings'
```

**Solución:**
Verificar que el archivo `config/database.yml` contiene la sección `table_settings`. Esta configuración se consolidó en la base de datos y ya no debe estar en los archivos `dataset.yml`.

### Problema: DBeaver no puede conectar

**Síntomas:**
- Connection timeout
- Connection refused

**Solución:**
1. **Verificar que PostgreSQL está ejecutándose:**
   ```bash
   docker ps | grep postgres
   ```

2. **Verificar puerto:**
   ```bash
   docker port mvp-config-driven-postgres-1
   ```

3. **Probar conexión desde línea de comandos:**
   ```bash
   docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse -c "SELECT 1;"
   ```

4. **Verificar firewall/antivirus** que pueda estar bloqueando el puerto 5432

---

## Comandos de Utilidad

### Reiniciar Todo el Entorno
```bash
# Detener todos los servicios
docker-compose down

# Limpiar volúmenes (CUIDADO: elimina todos los datos)
docker-compose down -v

# Reiniciar servicios
docker-compose up -d
```

### Ver Logs en Tiempo Real
```bash
# Logs de todos los servicios
docker-compose logs -f

# Logs específicos
docker-compose logs -f postgres
docker-compose logs -f runner
```

### Acceso Directo a Contenedores
```bash
# Acceso al contenedor de Spark/Python
docker exec -it mvp-config-driven-runner-1 bash

# Acceso al contenedor de PostgreSQL
docker exec -it mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse
```

---

## Conclusión

Este pipeline config-driven proporciona una solución completa para el procesamiento de datos con las siguientes características:

- ✅ **Carga de datos** desde archivos CSV con configuración flexible de rutas
- ✅ **Validación de calidad** con reglas configurables y cuarentena automática
- ✅ **Transformación y estandarización** de datos (renombrado, casting, valores por defecto)
- ✅ **Deduplicación** basada en claves configurables
- ✅ **Almacenamiento en capas** (Silver: Parquet en S3/MinIO, Gold: PostgreSQL)
- ✅ **Particionado automático** por fecha con overwrite dinámico
- ✅ **Esquemas dinámicos** basados en JSON con validación estricta
- ✅ **Organización por esquemas** (gold, metadata) en PostgreSQL
- ✅ **Metadatos de ejecución** y versionado de esquemas
- ✅ **Configuración centralizada** de table_settings en database.yml
- ✅ **Manejo de errores** y logging detallado
- ✅ **Arquitectura distribuida** con Spark Master/Worker

### Configuraciones Clave Actualizadas:

1. **Table Settings**: Centralizados en `config/database.yml` (no en dataset.yml)
2. **Esquemas de BD**: Uso de esquemas `gold` y `metadata` para organización
3. **Comando de ejecución**: `python spark_job_with_db.py dataset.yml env.yml database.yml environment`
4. **Datos de prueba**: Archivo `sample.csv` con 5 registros de ejemplo

Para cualquier problema adicional, revisar los logs de los contenedores y verificar la configuración de red y puertos.