# Guía de Reproducción - Casos de Uso Avanzados

## Descripción General

Esta guía proporciona los pasos exactos para reproducir los casos de uso avanzados que demuestran las capacidades completas del pipeline de datos implementado. Los casos de uso están diseñados para llevar el sistema al límite de sus capacidades actuales y validar el rendimiento en condiciones extremas.

## Prerrequisitos del Sistema

### Requisitos de Hardware
- **CPU**: Mínimo 8 cores, recomendado 16+ cores
- **RAM**: Mínimo 16GB, recomendado 32GB+
- **Almacenamiento**: Mínimo 100GB libres para datos temporales
- **Red**: Conexión estable para acceso a MinIO/S3

### Requisitos de Software
- **Python**: 3.8+
- **Apache Spark**: 3.4+
- **Java**: 11+
- **MinIO**: Para almacenamiento S3-compatible
- **PostgreSQL**: Para capa Gold
- **Dependencias Python**: Ver `requirements.txt`

### Variables de Entorno Requeridas
```bash
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_REGION=us-east-1
```

## Caso de Uso 1: Procesamiento de Pagos de Alto Volumen

### Objetivo
Demostrar el procesamiento de 1M+ registros con validaciones de calidad extremas, transformaciones complejas y escritura a múltiples capas.

### Configuración del Entorno

#### 1. Preparar Infraestructura
```bash
# Iniciar MinIO
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Iniciar PostgreSQL
docker run -d \
  --name postgres \
  -p 5432:5432 \
  -e POSTGRES_DB=pipeline_db \
  -e POSTGRES_USER=pipeline_user \
  -e POSTGRES_PASSWORD=pipeline_pass \
  postgres:13
```

#### 2. Crear Buckets en MinIO
```bash
# Acceder a MinIO Console: http://localhost:9001
# Crear buckets:
# - raw
# - silver
# - gold
```

#### 3. Configurar Base de Datos
```sql
-- Conectar a PostgreSQL y crear esquemas
CREATE SCHEMA IF NOT EXISTS payments_high_volume;
CREATE SCHEMA IF NOT EXISTS metadata;
```

### Ejecución del Caso de Uso

#### Paso 1: Generar Datos de Prueba
```bash
cd /path/to/mvp-config-driven
python generate_synthetic_data.py \
  --use-case high_volume \
  --records 1000000 \
  --batch-size 50000 \
  --error-rate 0.05 \
  --output-dir s3a://raw/casos-uso/payments-high-volume/
```

**Tiempo esperado**: 5-10 minutos
**Archivos generados**: 20 archivos CSV (~50MB cada uno)
**Registros con errores**: ~50,000 (5%)

#### Paso 2: Ejecutar Pipeline
```bash
python scripts/run_high_volume_case.py
```

**O ejecutar manualmente**:
```bash
python spark_job_with_db.py \
  --dataset config/datasets/casos_uso/payments_high_volume.yml \
  --environment development \
  --database-config config/database.yml \
  --verbose
```

### Métricas de Rendimiento Esperadas

#### Tiempo de Ejecución
- **Generación de datos**: 5-10 minutos
- **Lectura y parsing**: 2-3 minutos
- **Standardización**: 3-5 minutos
- **Validaciones de calidad**: 5-8 minutos
- **Escritura Silver**: 2-3 minutos
- **Transformaciones Gold**: 3-5 minutos
- **Escritura Gold**: 2-4 minutos
- **Total**: 22-38 minutos

#### Uso de Recursos
- **Memoria máxima**: 8-12GB
- **CPU utilización**: 70-90%
- **I/O disco**: 500MB/s picos
- **Red**: 100-200MB/s

#### Calidad de Datos
- **Registros procesados**: 1,000,000
- **Registros válidos**: ~950,000 (95%)
- **Registros en cuarentena**: ~50,000 (5%)
- **Registros en Silver**: ~950,000
- **Registros en Gold**: ~900,000 (después de filtros de negocio)

#### Validaciones Aplicadas
- **Formato payment_id**: 100% validado
- **Rangos de monto**: 100% validado
- **Monedas permitidas**: 100% validado
- **Fechas coherentes**: 100% validado
- **Reglas de negocio**: 15+ reglas aplicadas

### Resultados Esperados

#### Archivos en Silver Layer
```
s3a://silver/payments-high-volume/
├── year=2024/
│   ├── month=01/
│   │   ├── part-00000.parquet
│   │   ├── part-00001.parquet
│   │   └── ...
│   └── month=02/
└── _SUCCESS
```

#### Tablas en Gold Layer
```sql
-- Tabla principal
SELECT COUNT(*) FROM payments_high_volume.payments_high_volume;
-- Resultado esperado: ~900,000 registros

-- Verificar columnas añadidas
SELECT DISTINCT data_source, pipeline_version 
FROM payments_high_volume.payments_high_volume;
```

#### Archivos de Cuarentena
```
s3a://raw/quarantine/payments-high-volume/
├── year=2024/
│   ├── month=01/
│   │   ├── day=15/
│   │   │   ├── quarantine-001.parquet
│   │   │   └── ...
```

## Caso de Uso 2: Procesamiento Multi-Formato con Validación Extrema

### Objetivo
Demostrar el procesamiento de datos JSON complejos con validaciones en múltiples niveles, manejo de registros corruptos y transformaciones de datos semi-estructurados.

### Ejecución del Caso de Uso

#### Paso 1: Generar Datos de Prueba Multi-Formato
```bash
python generate_synthetic_data.py \
  --use-case multiformat \
  --records 500000 \
  --batch-size 25000 \
  --error-rate 0.10 \
  --output-dir s3a://raw/casos-uso/events-multiformat/
```

**Tiempo esperado**: 3-6 minutos
**Archivos generados**: 20 archivos JSON (~25MB cada uno)
**Registros con errores**: ~50,000 (10%)

#### Paso 2: Ejecutar Pipeline Multi-Formato
```bash
python scripts/run_multiformat_case.py
```

**O ejecutar manualmente**:
```bash
python spark_job_with_db.py \
  --dataset config/datasets/casos_uso/events_multiformat.yml \
  --environment development \
  --database-config config/database.yml \
  --verbose
```

### Métricas de Rendimiento Esperadas

#### Tiempo de Ejecución
- **Generación de datos**: 3-6 minutos
- **Parsing JSON**: 3-5 minutos
- **Validación de esquemas**: 4-6 minutos
- **Validaciones de calidad**: 6-10 minutos
- **Transformaciones**: 3-5 minutos
- **Escritura Silver**: 2-3 minutos
- **Transformaciones Gold**: 4-6 minutos
- **Escritura Gold**: 2-4 minutos
- **Total**: 27-45 minutos

#### Uso de Recursos
- **Memoria máxima**: 6-10GB
- **CPU utilización**: 60-80%
- **I/O disco**: 300MB/s picos
- **Red**: 80-150MB/s

#### Calidad de Datos
- **Registros procesados**: 500,000
- **Registros JSON válidos**: ~450,000 (90%)
- **Registros corruptos**: ~50,000 (10%)
- **Registros en cuarentena**: ~75,000 (15%)
- **Registros en Silver**: ~425,000
- **Registros en Gold**: ~400,000

#### Validaciones Complejas Aplicadas
- **Formato event_id**: Patrón EVT-timestamp-hash
- **Validaciones condicionales**: 4 reglas if-then
- **JSON anidado**: Validación de 3 campos JSON
- **Lógica de negocio**: 8 reglas específicas por tipo de evento
- **Integridad referencial**: 5 validaciones cruzadas

### Resultados Esperados

#### Archivos en Silver Layer
```
s3a://silver/events-multiformat/
├── year=2024/
│   ├── month=01/
│   │   ├── day=15/
│   │   │   ├── event_type=PAGE_VIEW/
│   │   │   ├── event_type=CLICK/
│   │   │   ├── event_type=PURCHASE/
│   │   │   └── ...
```

#### Tablas en Gold Layer
```sql
-- Tabla principal con enriquecimientos
SELECT 
  COUNT(*) as total_events,
  COUNT(DISTINCT event_type) as event_types,
  AVG(data_quality_score) as avg_quality_score
FROM events_multiformat.events_multiformat;

-- Distribución por tipo de evento
SELECT event_type, COUNT(*) as count
FROM events_multiformat.events_multiformat
GROUP BY event_type
ORDER BY count DESC;
```

## Validación de Resultados

### Scripts de Validación Automática

#### Validar Caso de Alto Volumen
```bash
python scripts/validate_high_volume_results.py
```

#### Validar Caso Multi-Formato
```bash
python scripts/validate_multiformat_results.py
```

### Verificaciones Manuales

#### 1. Verificar Archivos Generados
```bash
# Contar archivos en Silver
aws s3 ls s3://silver/payments-high-volume/ --recursive --endpoint-url http://localhost:9000

# Verificar tamaños
aws s3 ls s3://silver/payments-high-volume/ --recursive --human-readable --endpoint-url http://localhost:9000
```

#### 2. Verificar Tablas en Base de Datos
```sql
-- Verificar esquemas creados
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name LIKE '%high_volume%' OR schema_name LIKE '%multiformat%';

-- Verificar tablas creadas
SELECT table_name, table_rows 
FROM information_schema.tables 
WHERE table_schema IN ('payments_high_volume', 'events_multiformat');
```

#### 3. Verificar Logs de Ejecución
```bash
# Revisar logs de ejecución
tail -f high_volume_execution_*.log
tail -f multiformat_execution_*.log

# Buscar errores
grep -i error *.log
grep -i exception *.log
```

## Métricas de Éxito

### Criterios de Aceptación

#### Caso de Alto Volumen
- ✅ Procesar 1M+ registros en menos de 40 minutos
- ✅ Tasa de éxito > 90%
- ✅ Uso de memoria < 16GB
- ✅ Todas las validaciones aplicadas correctamente
- ✅ Particionado por fecha funcionando
- ✅ Tablas Gold creadas con esquema correcto

#### Caso Multi-Formato
- ✅ Procesar 500K registros JSON en menos de 45 minutos
- ✅ Detectar y manejar registros corruptos
- ✅ Aplicar validaciones condicionales complejas
- ✅ Particionado multi-dimensional funcionando
- ✅ Enriquecimiento de datos aplicado
- ✅ Transformaciones de JSON anidado exitosas

### Indicadores de Rendimiento (KPIs)

#### Throughput
- **Alto Volumen**: 25,000-45,000 registros/minuto
- **Multi-Formato**: 11,000-18,500 registros/minuto

#### Latencia
- **Tiempo de respuesta promedio**: < 2 segundos por batch
- **Tiempo de escritura**: < 30 segundos por partición

#### Calidad
- **Tasa de detección de errores**: > 95%
- **Precisión de validaciones**: 100%
- **Integridad de datos**: 100%

## Solución de Problemas

### Problemas Comunes

#### 1. Error de Memoria
```bash
# Aumentar memoria de Spark
export SPARK_DRIVER_MEMORY=8g
export SPARK_EXECUTOR_MEMORY=4g
```

#### 2. Error de Conexión a MinIO
```bash
# Verificar que MinIO esté ejecutándose
docker ps | grep minio
curl http://localhost:9000/minio/health/live
```

#### 3. Error de Base de Datos
```bash
# Verificar conexión a PostgreSQL
psql -h localhost -U pipeline_user -d pipeline_db -c "SELECT 1;"
```

#### 4. Archivos No Encontrados
```bash
# Verificar buckets en MinIO
aws s3 ls s3://raw/ --endpoint-url http://localhost:9000
aws s3 ls s3://silver/ --endpoint-url http://localhost:9000
```

### Logs de Depuración

#### Habilitar Logs Detallados
```bash
export SPARK_LOG_LEVEL=DEBUG
export PYTHONPATH=$PYTHONPATH:$(pwd)
```

#### Ubicación de Logs
- **Pipeline**: `*.log` en directorio de ejecución
- **Spark**: `$SPARK_HOME/logs/`
- **MinIO**: Docker logs con `docker logs minio`
- **PostgreSQL**: Docker logs con `docker logs postgres`

## Conclusión

Esta guía proporciona todos los pasos necesarios para reproducir exitosamente los casos de uso avanzados que demuestran las capacidades completas del pipeline implementado. Los casos de uso están diseñados para validar:

1. **Escalabilidad**: Procesamiento de volúmenes masivos de datos
2. **Robustez**: Manejo de errores y datos corruptos
3. **Flexibilidad**: Soporte para múltiples formatos y esquemas
4. **Calidad**: Validaciones exhaustivas en múltiples niveles
5. **Rendimiento**: Optimizaciones para procesamiento eficiente

El éxito en la ejecución de estos casos de uso confirma que el pipeline está listo para entornos de producción con cargas de trabajo exigentes.