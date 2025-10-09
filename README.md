# MVP Config-Driven Data Pipeline

Este proyecto implementa un **pipeline de datos dinámico y flexible** basado en **Apache Spark + PostgreSQL**, diseñado para procesar datos a través de múltiples capas (Source → Silver → Gold) con configuración completamente externalizada.

## 🎯 Características Principales

- ✅ **Pipeline Multi-Capa**: Source → Silver (Parquet) → Gold (PostgreSQL)
- ✅ **Configuración Externalizada**: Esquemas JSON, reglas de calidad YAML
- ✅ **Validación de Calidad**: Reglas configurables con cuarentena automática
- ✅ **Esquemas Dinámicos**: Creación automática de tablas desde JSON Schema
- ✅ **Particionado Inteligente**: Por fecha con columnas automáticas
- ✅ **Transformaciones Configurables**: Renombrado, casting, valores por defecto
- ✅ **Deduplicación**: Basada en claves y ordenamiento configurables
- ✅ **Containerizado**: Docker Compose para desarrollo y producción

## 🏗️ Arquitectura

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Source    │───▶│    Silver    │───▶│    Gold     │
│   (CSV)     │    │  (Parquet)   │    │(PostgreSQL) │
└─────────────┘    └──────────────┘    └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│ Validación  │    │Particionado  │    │ Esquemas    │
│ de Calidad  │    │por Fecha     │    │ Dinámicos   │
└─────────────┘    └──────────────┘    └─────────────┘
```

```
mvp-config-driven/
├── config/
│   ├── env.yml                    # Variables de entorno
│   ├── database.yml               # Configuración de PostgreSQL
│   └── datasets/                  # Configuraciones de datasets
│       └── finanzas/
│           ├── payments_v1/       # Dataset payments versión 1
│           │   ├── dataset.yml    # Configuración del pipeline
│           │   ├── dataset_with_gold.yml  # Configuración con BD
│           │   ├── schema.json    # Esquema JSON del dataset
│           │   └── expectations.yml # Reglas de calidad
│           └── payments_v2/       # Dataset payments versión 2
├── data/                          # Datos de entrada
│   └── raw/
│       └── payments/              # Archivos CSV de pagos
│   └── sample_payments.csv        # Datos de ejemplo
├── pipelines/
│   ├── spark_job_with_db.py       # Pipeline principal
│   ├── db_manager.py              # Gestor de base de datos
│   └── schema_mapper.py           # Mapeo de esquemas
├── scripts/
│   └── run_pipeline.py            # Script de ejecución
├── docs/
│   ├── MANUAL_TESTING_GUIDE.md    # Guía de pruebas manuales
│   └── PROJECT_STRUCTURE.md       # Documentación del proyecto
└── docker-compose.yml             # Servicios Docker
```

## 🚀 Inicio Rápido

### Prerrequisitos

- **Docker** y **Docker Compose**
- **Python 3.8+** con PySpark
- **DBeaver** (opcional, para visualización)

### 1. Levantar la Infraestructura

```bash
# Iniciar PostgreSQL
docker-compose up -d

# Verificar que esté funcionando
docker-compose ps
```

### 2. Ejecutar el Pipeline

```bash
# Ejecutar pipeline completo
python pipelines/spark_job_with_db.py config/datasets/finanzas/payments_v1/dataset_with_gold.yml config/env.yml config/database.yml development

```bash
docker-compose exec runner python pipelines/spark_job_with_db.py config/datasets/finanzas/payments_v1/dataset_with_gold.yml config/env.yml config/database.yml development
```
### 3. Verificar Resultados

```bash
# Conectar a PostgreSQL y verificar datos
docker exec -it mvp-postgres psql -U testuser -d testdb -c "SELECT * FROM test_payments_v1 LIMIT 5;"
```

## 📋 Documentación Completa

- **[Guía de Pruebas Manuales](docs/MANUAL_TESTING_GUIDE.md)**: Instrucciones paso a paso para probar el pipeline
- **[Estructura del Proyecto](docs/PROJECT_STRUCTURE.md)**: Documentación detallada de la arquitectura

## 🔧 Configuración

### Variables de Entorno (`config/env.yml`)

```yaml
spark:
  app_name: "ConfigDrivenPipeline"
  master: "local[*]"
  
paths:
  silver_base: "./silver_data"
  quarantine_base: "./quarantine_data"
```

### Configuración de Base de Datos (`config/database.yml`)

```yaml
postgresql:
  host: localhost
  port: 5432
  database: testdb
  username: testuser
  password: testpass
```

## 🎯 Casos de Uso

### Agregar Nuevo Dataset

1. Crear directorio en `config/datasets/[categoria]/[dataset_name]/`
2. Crear esquema JSON en `schema.json`
3. Definir reglas de calidad en `expectations.yml`
4. Configurar pipeline en `dataset.yml` o `dataset_with_gold.yml`
5. Ejecutar pipeline

### Modificar Transformaciones

1. Actualizar archivo de configuración del dataset con nuevas reglas
2. Ejecutar pipeline sin cambios de código

### Conectar Nueva Base de Datos

1. Actualizar `config/database.yml`
2. Modificar `db_manager.py` si es necesario

## 🛠️ Desarrollo y Extensión

### Agregar Nuevas Transformaciones

Editar archivo de configuración del dataset (ej. `config/datasets/finanzas/payments_v1/dataset.yml`):

```yaml
standardization:
  rename:
    - { from: "old_column", to: "new_column" }
  casts:
    - { column: "amount", to: "decimal(18,2)", on_error: "null" }
  defaults:
    - { column: "currency", value: "USD" }
```

### Modificar Reglas de Calidad

Editar archivo de expectations del dataset (ej. `config/datasets/finanzas/payments_v1/expectations.yml`):

```yaml
expectations:
  - column: "amount"
    rule: ">= 0"
    action: "quarantine"
  - column: "payment_id"
    rule: "not null"
    action: "reject"
```

### Cambiar Esquema de Base de Datos

1. Actualizar <mcfile name="schema.json" path="test_data/schema.json"></mcfile>
2. El pipeline creará automáticamente la tabla con el nuevo esquema

## 🔍 Monitoreo y Logs

### Ver Logs del Pipeline

```bash
# Logs de Spark
python pipelines/spark_job_with_db.py ... --verbose

# Logs de PostgreSQL
docker logs mvp-postgres
```

### Verificar Estado de la Base de Datos

```bash
# Conectar a PostgreSQL
docker exec -it mvp-postgres psql -U testuser -d testdb

# Ver tablas
\dt

# Ver datos
SELECT * FROM test_payments_v1 LIMIT 10;
```

## 🚨 Troubleshooting

### Problemas Comunes

#### 1. Error de Conexión a PostgreSQL

```bash
# Verificar que el contenedor esté ejecutándose
docker-compose ps

# Reiniciar servicios
docker-compose down && docker-compose up -d
```

#### 2. Error de Esquema JSON

- Verificar que el archivo `schema.json` del dataset tenga formato válido
- Usar herramientas online para validar JSON

#### 3. Datos No Aparecen en la Base de Datos

- Verificar que el archivo CSV en `data/raw/` tenga datos válidos
- Revisar logs del pipeline para errores de calidad
- Verificar que las reglas de `expectations.yml` del dataset no rechacen todos los datos

## 📈 Próximos Pasos

- [ ] **Orquestación**: Integración con Apache Airflow
- [ ] **Streaming**: Soporte para Apache Kafka
- [ ] **Monitoreo**: Dashboard con métricas del pipeline
- [ ] **Alertas**: Notificaciones automáticas en caso de fallos
- [ ] **Escalabilidad**: Despliegue en Kubernetes

## 🤝 Contribuciones

1. Fork del repositorio
2. Crear rama feature: `git checkout -b feature/nueva-funcionalidad`
3. Commit cambios: `git commit -am 'Agregar nueva funcionalidad'`
4. Push a la rama: `git push origin feature/nueva-funcionalidad`
5. Crear Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.

---

**¿Necesitas ayuda?** Consulta la [Guía de Pruebas Manuales](docs/MANUAL_TESTING_GUIDE.md) para instrucciones detalladas paso a paso.
