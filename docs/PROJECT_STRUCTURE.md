# Estructura del Proyecto - Pipeline Config-Driven

## Descripción General

Este documento describe la estructura final del proyecto después de la limpieza y optimización, explicando el propósito de cada directorio y archivo.

## Estructura de Directorios

```
mvp-config-driven/
├── config/                          # Configuraciones del pipeline
│   ├── database.yml                 # Configuración de base de datos (producción)
│   ├── env.yml                      # Variables de entorno
│   └── datasets/                    # Esquemas y configuraciones por dataset
│       └── finanzas/
│           └── payments_v1/
│               ├── schema.json      # Esquema JSON del dataset
│               └── expectations.yml # Reglas de calidad de datos
│
├── data/                            # Datos de entrada (desarrollo)
│   └── raw/
│       └── payments/
│           └── sample.csv           # Datos de ejemplo
│
├── docs/                            # Documentación
│   ├── GOLD_LAYER_DATABASE.md      # Documentación de la capa Gold
│   ├── MANUAL_TESTING_GUIDE.md     # Guía de pruebas manuales
│   └── PROJECT_STRUCTURE.md        # Este documento
│
├── pipelines/                       # Código del pipeline
│   ├── database/                    # Módulos de base de datos
│   │   ├── __init__.py
│   │   ├── db_manager.py           # Gestor de base de datos
│   │   └── schema_mapper.py        # Mapeo de tipos de datos
│   ├── common.py                   # Funciones comunes compartidas
│   ├── spark_job.py                # Pipeline básico (sin BD)
│   └── spark_job_with_db.py        # Pipeline completo con BD
│
├── scripts/                         # Scripts de utilidad
│   ├── db/
│   │   └── init.sql                # Inicialización de BD PostgreSQL
│   ├── generate_big_payments.py    # Generador de datos de prueba grandes
│   └── validate_gold_layer.py     # Validador de capa Gold
│
├── data/                           # Datos de entrada del sistema
│   └── raw/                        # Datos en bruto
│       └── payments/               # Archivos CSV de pagos
│           ├── sample.csv          # Muestra pequeña de datos
│           └── big_sample.csv      # Muestra grande de datos
│
├── ci/                             # Configuración de CI/CD
│   ├── README.md
│   ├── check_config.sh
│   ├── docker-compose.override.yml
│   ├── lint.yml
│   └── test_dataset.yml
│
├── .env                            # Variables de entorno locales
├── .env.example                    # Plantilla de variables de entorno
├── docker-compose.yml              # Configuración de Docker
├── Makefile                        # Comandos de automatización
├── postgresql-42.7.2.jar          # Driver JDBC de PostgreSQL
├── requirements.txt                # Dependencias de Python
└── README.md                       # Documentación principal
```

## Archivos Principales

### Configuración

#### `config/env.yml`
Variables de entorno para diferentes ambientes (desarrollo, producción).

```yaml
default:
  spark:
    app_name: "ConfigDrivenPipeline"
    master: "local[*]"
    config:
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"
```

#### `config/database.yml`
Configuración de conexiones a base de datos para todos los entornos (development, production).

```yaml
development:
  type: postgresql
  host: postgres
  port: 5432
  database: data_warehouse
  username: postgres
  password: postgres123
  connection_params:
    connect_timeout: 30
```

### Esquemas y Configuraciones de Datasets

#### `config/datasets/finanzas/payments_v1/schema.json`
Define el esquema del dataset incluyendo tipos de datos, restricciones y validaciones.

#### `config/datasets/finanzas/payments_v1/expectations.yml`
Reglas de calidad de datos para validación.

#### `config/datasets/finanzas/payments_v1/dataset_with_gold.yml`
Configuración completa del pipeline para el dataset con integración a base de datos.

```yaml
id: payments_v1

source:
  input_format: csv
  path: "s3a://raw/payments/2025/09/26/*.csv"
  options:
    header: "true"
    inferSchema: "true"

standardization:
  timezone: America/Bogota
  rename:
    - { from: customerId, to: customer_id }
  casts:
    - { column: amounts, to: "decimal(18,2)", on_error: null }
  # ... más configuraciones

output:
  silver:
    format: parquet
    path: "s3a://silver/payments_v1/"
    partition_by: [year, month]
    partition_from: payment_date
  gold:
    enabled: true
    table_name: "payments_v1"
    mode: "append"
```

### Código del Pipeline

#### `pipelines/common.py`
Módulo de funciones compartidas que incluye:
- Normalización de tipos de datos (`norm_type`)
- Parseo de expresiones de ordenamiento (`parse_order`)
- Casting seguro de columnas (`safe_cast`)
- Configuración automática de S3A (`maybe_config_s3a`)

#### `pipelines/spark_job_with_db.py`
Pipeline principal que incluye:
- Carga de datos desde fuentes configurables
- Transformaciones y estandarización
- Validación de calidad de datos
- Escritura a capa Silver (Parquet)
- Escritura a capa Gold (PostgreSQL)

#### `pipelines/database/db_manager.py`
Gestor de base de datos que proporciona:
- Conexión a PostgreSQL
- Creación dinámica de tablas desde esquemas JSON
- Escritura de DataFrames a base de datos
- Manejo de tipos de datos y constraints

#### `pipelines/database/schema_mapper.py`
Mapeo entre tipos de datos JSON Schema y tipos de PostgreSQL.

### Scripts de Utilidad

#### `scripts/validate_gold_layer.py`
Script para validar que los datos en la capa Gold cumplen con las expectativas.

#### `scripts/generate_big_payments.py`
Generador de datasets grandes para pruebas de rendimiento.

#### `scripts/db/init.sql`
Script de inicialización de PostgreSQL que crea esquemas y tablas necesarias.

## Flujo de Datos

```
[CSV Source] 
    ↓
[Source Layer] → Carga y validación inicial
    ↓
[Standardization] → Transformaciones y limpieza
    ↓
[Quality Checks] → Validación de reglas de negocio
    ↓
[Silver Layer] → Almacenamiento en Parquet (particionado)
    ↓
[Gold Layer] → Almacenamiento en PostgreSQL (optimizado para consultas)
```

## Configuración por Capas

### Capa Source
- **Formato:** CSV, JSON, Parquet (configurable)
- **Ubicación:** Archivos locales, S3, HDFS
- **Opciones:** Headers, delimitadores, esquemas

### Capa Silver
- **Formato:** Parquet (optimizado para analytics)
- **Particionado:** Por fecha (year/month)
- **Metadatos:** run_id, ingestion_timestamp
- **Compresión:** Snappy (por defecto)

### Capa Gold
- **Formato:** PostgreSQL (optimizado para consultas)
- **Esquema:** Dinámico basado en JSON Schema
- **Constraints:** Primary keys, check constraints, not null
- **Índices:** Automáticos en primary keys

## Variables de Entorno

### Desarrollo Local
```bash
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=data_warehouse
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres123

# Spark
SPARK_MASTER=local[*]
SPARK_APP_NAME=ConfigDrivenPipeline
```

### Docker
Las variables se configuran automáticamente en `docker-compose.yml`.

## Comandos Principales

### Desarrollo
```bash
# Iniciar entorno
docker-compose up -d

# Ejecutar pipeline
docker exec mvp-config-driven-runner-1 python /mvp/pipelines/spark_job_with_db.py \
  /mvp/test_data/dataset_test.yml \
  /mvp/config/env.yml \
  /mvp/test_data/database_test.yml \
  default

# Verificar resultados
docker exec mvp-config-driven-postgres-1 psql -U postgres -d data_warehouse \
  -c "SELECT COUNT(*) FROM test_payments_v1;"
```

### Producción
```bash
# Usar configuraciones de producción
python pipelines/spark_job_with_db.py \
  config/datasets/finanzas/payments_v1/dataset.yml \
  config/env.yml \
  config/database.yml \
  production
```

## Extensibilidad

### Agregar Nuevo Dataset
1. Crear directorio en `config/datasets/[dominio]/[dataset_name]/`
2. Definir `schema.json` con el esquema del dataset
3. Crear `expectations.yml` con reglas de calidad
4. Configurar `dataset.yml` con transformaciones y salidas

### Agregar Nueva Base de Datos
1. Extender `schema_mapper.py` con mapeos de tipos
2. Implementar driver específico en `db_manager.py`
3. Actualizar configuraciones en `database.yml`

### Agregar Nuevas Transformaciones
1. Extender funciones en `spark_job_with_db.py`
2. Agregar configuraciones en la sección `standardization`
3. Documentar en esquemas JSON si afectan la estructura

## Monitoreo y Logging

### Logs del Pipeline
- **Nivel:** INFO por defecto
- **Formato:** `[layer] message`
- **Ubicación:** stdout (capturado por Docker)

### Métricas
- Número de registros procesados por capa
- Estadísticas de calidad de datos
- Tiempos de ejecución por etapa
- Errores y warnings

### Alertas
- Fallos en validación de calidad
- Errores de conexión a base de datos
- Problemas de escritura en capas

## Seguridad

### Credenciales
- Variables de entorno para passwords
- Archivos `.env` excluidos del control de versiones
- Conexiones encriptadas en producción

### Acceso a Datos
- Principio de menor privilegio
- Usuarios específicos por ambiente
- Auditoría de accesos en base de datos

## Mantenimiento

### Limpieza de Datos
- Particiones antiguas en Silver
- Logs de ejecución
- Datos de cuarentena

### Actualizaciones
- Esquemas versionados
- Migraciones de base de datos
- Compatibilidad hacia atrás

### Backup
- Configuraciones en control de versiones
- Datos críticos en base de datos
- Esquemas y metadatos