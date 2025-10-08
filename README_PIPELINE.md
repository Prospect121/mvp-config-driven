# Pipeline de Procesamiento de Datos - Solución Implementada

## Resumen de la Solución

Se ha implementado exitosamente un pipeline de procesamiento de datos usando **Azure Data Factory** con validaciones nativas y transformaciones optimizadas. La solución utiliza una arquitectura simplificada pero robusta que evita los problemas de cuota de Azure Functions.

## Arquitectura Implementada

### Componentes Principales

1. **Azure Data Factory** - Orquestación del pipeline
2. **Azure Data Lake Storage Gen2** - Almacenamiento de datos
3. **Azure SQL Database** - Base de datos transaccional
4. **Azure Key Vault** - Gestión de secretos
5. **Azure Event Hub** - Ingesta de datos en tiempo real
6. **Log Analytics Workspace** - Monitoreo y logging

### Pipeline Principal: `MainProcessingPipeline`

El pipeline implementado incluye las siguientes actividades:

#### 1. ValidateInputData
- **Tipo**: Validation Activity
- **Función**: Valida que los archivos de entrada existan y cumplan criterios mínimos
- **Configuración**:
  - Timeout: 5 minutos
  - Tamaño mínimo: 1 byte
  - Intervalo de verificación: 10 segundos

#### 2. CopyAndTransformData
- **Tipo**: Copy Activity
- **Función**: Copia y transforma datos de CSV a Parquet
- **Características**:
  - Conversión automática de tipos
  - Manejo de filas incompatibles
  - Logging de advertencias
  - Optimización con 2 DIUs (Data Integration Units)
  - Comportamiento de copia: FlattenHierarchy

#### 3. LogProcessingMetrics
- **Tipo**: Set Variable Activity
- **Función**: Captura métricas de procesamiento
- **Métricas registradas**:
  - Número de filas copiadas
  - Número de archivos procesados
  - Duración del procesamiento

## Configuración de Validaciones

### Archivo de Configuración: `config/pipeline_config.json`

```json
{
  "validation_rules": {
    "file_validation": {
      "minimum_size_bytes": 1,
      "maximum_size_mb": 100,
      "allowed_extensions": [".csv", ".txt"],
      "encoding": "utf-8"
    },
    "data_validation": {
      "skip_incompatible_rows": true,
      "log_level": "Warning",
      "max_error_percentage": 5,
      "required_columns": ["id", "timestamp", "value"]
    }
  }
}
```

## Estructura de Datos

### Arquitectura Medallion Implementada

```
Storage Account (pdgdevsa001)
├── raw/
│   └── csv/                 # Datos de entrada (Bronze)
├── silver/
│   └── processed/           # Datos procesados (Silver)
├── quarantine/              # Datos con errores
└── logs/                    # Logs de procesamiento
```

### Datasets Configurados

1. **CsvSourceDataset**
   - Formato: Delimited Text (CSV)
   - Ubicación: `raw/csv/`
   - Encoding: UTF-8

2. **ParquetSinkDataset**
   - Formato: Parquet
   - Ubicación: `silver/processed/`
   - Compresión: Snappy

## Seguridad y Acceso

### Managed Identity
- Data Factory utiliza Managed Identity para acceso seguro
- Permisos configurados para:
  - Storage Blob Data Contributor
  - Key Vault Secrets User
  - SQL DB Contributor

### Key Vault Secrets
- `storage-account-key`: Clave de la cuenta de almacenamiento
- `sql-connection-string`: Cadena de conexión a SQL Database
- `eventhub-connection-string`: Cadena de conexión a Event Hub

## Monitoreo y Logging

### Application Insights
- Logging automático de actividades del pipeline
- Métricas de rendimiento
- Alertas configurables

### Log Analytics
- Logs centralizados de todos los componentes
- Consultas KQL para análisis avanzado
- Dashboards de monitoreo

## Pruebas y Validación

### Script de Prueba: `scripts/test_pipeline.ps1`

Para ejecutar las pruebas de integración:

```powershell
.\scripts\test_pipeline.ps1 `
    -ResourceGroupName "rg-pdg-datapipe-dev-001" `
    -DataFactoryName "pdg-datapipe-dev-df-001" `
    -StorageAccountName "pdgdevsa001"
```

### Funcionalidades del Script de Prueba

1. ✅ Verificación de Azure CLI
2. ✅ Creación de datos de prueba
3. ✅ Subida de archivos a Storage
4. ✅ Verificación de existencia del pipeline
5. ✅ Ejecución del pipeline
6. ✅ Monitoreo de la ejecución
7. ✅ Verificación de resultados
8. ✅ Limpieza de archivos temporales

## Despliegue

### Terraform
La infraestructura se despliega usando Terraform:

```bash
cd terraform
terraform init
terraform plan
terraform apply -auto-approve
```

### Recursos Desplegados
- 1 Resource Group
- 1 Data Factory con pipeline configurado
- 1 Storage Account (ADLS Gen2)
- 1 SQL Server y Database
- 1 Event Hub Namespace
- 1 Key Vault
- 1 Log Analytics Workspace

## Ventajas de la Solución Implementada

### ✅ Ventajas

1. **Sin problemas de cuota**: Utiliza servicios nativos de Azure
2. **Costo optimizado**: No requiere Azure Functions o Batch
3. **Escalabilidad**: Data Factory escala automáticamente
4. **Monitoreo integrado**: Logging y métricas nativas
5. **Seguridad**: Managed Identity y Key Vault
6. **Mantenimiento mínimo**: Servicios completamente gestionados

### 🔧 Características Técnicas

- **Validación de datos**: Actividad de validación nativa
- **Transformación**: Copy Activity con conversión de tipos
- **Manejo de errores**: Skip de filas incompatibles
- **Logging**: Registro detallado de métricas
- **Formato optimizado**: Conversión a Parquet con compresión

## Próximos Pasos

1. **Configurar alertas** en Azure Monitor
2. **Implementar triggers** para ejecución automática
3. **Agregar más validaciones** según necesidades del negocio
4. **Configurar CI/CD** para despliegues automatizados
5. **Implementar data lineage** para trazabilidad

## Soporte y Mantenimiento

### Logs y Troubleshooting
- Revisar logs en Azure Data Factory Monitor
- Consultar métricas en Application Insights
- Verificar alertas en Azure Monitor

### Contacto
Para soporte técnico, contactar al equipo de datos de Prodigio.

---

**Fecha de implementación**: $(Get-Date -Format "yyyy-MM-dd")  
**Versión**: 1.0.0  
**Estado**: ✅ Desplegado y funcional