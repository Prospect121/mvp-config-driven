# Guía de Pruebas y Configuración de Recursos Azure

## 📋 Descripción General

Esta guía proporciona instrucciones detalladas para probar y configurar los recursos de Azure desplegados en el proyecto MVP Config-Driven Pipeline. Incluye scripts automatizados, archivos de configuración y datos de prueba.

## 🗂️ Estructura de Archivos

```
mvp-config-driven/
├── config/
│   ├── data_factory_config.yml      # Configuración de Azure Data Factory
│   └── eventhub_config.yml          # Configuración de Azure Event Hub
├── scripts/
│   ├── configure_azure_resources.py # Script Python para configuración automática
│   └── test_azure_resources.ps1     # Script PowerShell para pruebas
├── test_data/
│   └── sample_data.csv              # Datos de ejemplo para pruebas
└── README_TESTING.md                # Esta guía
```

## 🚀 Inicio Rápido

### 1. Verificar Recursos con PowerShell

```powershell
# Navegar al directorio de scripts
cd scripts

# Ejecutar pruebas completas
.\test_azure_resources.ps1

# Ejecutar con parámetros específicos
.\test_azure_resources.ps1 -SubscriptionId "tu-subscription-id" -ResourceGroupName "tu-resource-group"
```

### 2. Configurar Recursos con Python

```bash
# Instalar dependencias
pip install azure-identity azure-mgmt-datafactory azure-mgmt-eventhub azure-mgmt-storage azure-mgmt-sql azure-keyvault-secrets pyyaml

# Ejecutar configuración
cd scripts
python configure_azure_resources.py
```

## 📊 Datos de Prueba

### Archivo CSV de Ejemplo

El archivo `test_data/sample_data.csv` contiene 20 registros de ejemplo con la siguiente estructura:

| Campo | Descripción | Ejemplo |
|-------|-------------|---------|
| id | Identificador único | 1, 2, 3... |
| nombre | Nombre completo | Juan Pérez |
| email | Correo electrónico | juan.perez@email.com |
| fecha_registro | Fecha de registro | 2024-01-15 |
| edad | Edad del usuario | 28 |
| ciudad | Ciudad de residencia | Madrid |
| pais | País | España |
| estado | Estado del usuario | activo |
| ingresos_anuales | Ingresos anuales | 45000 |
| categoria_cliente | Categoría | premium/standard |

### Uso de los Datos

1. **Carga Automática**: El script de PowerShell sube automáticamente el CSV al Storage Account
2. **Procesamiento**: Los datos pueden ser procesados por Data Factory pipelines
3. **Análisis**: Los resultados se almacenan en SQL Database para análisis

## 🔧 Configuración Detallada

### Azure Data Factory

El archivo `config/data_factory_config.yml` define:

- **Linked Services**: Conexiones a Storage Account, SQL Database y Event Hub
- **Datasets**: Definiciones de datos de entrada y salida
- **Pipelines**: Flujos de procesamiento de datos
- **Triggers**: Programación automática de ejecuciones

#### Ejemplo de Configuración de Pipeline

```yaml
pipelines:
  csv_ingestion_pipeline:
    name: "CSV-Ingestion-Pipeline"
    description: "Pipeline para ingerir datos CSV desde Storage a SQL Database"
    activities:
      - name: "Copy-CSV-to-SQL"
        type: "Copy"
        source:
          type: "DelimitedTextSource"
          dataset: "csv_input_dataset"
        sink:
          type: "AzureSqlSink"
          dataset: "sql_output_dataset"
```

### Azure Event Hub

El archivo `config/eventhub_config.yml` configura:

- **Namespace**: Contenedor para Event Hubs
- **Event Hubs**: Canales de eventos individuales
- **Consumer Groups**: Grupos de consumidores para procesamiento paralelo
- **Capture**: Configuración para captura automática de eventos

#### Ejemplo de Configuración de Event Hub

```yaml
event_hubs:
  data_events:
    name: "data-events-hub"
    partition_count: 4
    message_retention_days: 7
    consumer_groups:
      - name: "analytics-group"
        user_metadata: "Grupo para análisis en tiempo real"
```

## 🧪 Pruebas Paso a Paso

### Prueba 1: Verificación de Recursos

```powershell
# 1. Ejecutar script de pruebas
.\test_azure_resources.ps1

# 2. Verificar output esperado:
# ✅ Autenticación: OK
# ✅ Grupo de Recursos: OK
# ✅ Storage Account: OK
# ✅ SQL Server: OK
# ✅ Data Factory: OK
# ✅ Event Hub: OK
# ✅ Key Vault: OK
# ✅ Log Analytics: OK
# ✅ Carga de Datos: OK
```

### Prueba 2: Carga y Procesamiento de CSV

```powershell
# 1. El script automáticamente:
#    - Verifica el archivo CSV
#    - Crea contenedor en Storage Account
#    - Sube el archivo con timestamp
#    - Confirma la carga exitosa

# 2. Verificar en Azure Portal:
#    - Ir a Storage Account > Containers > test-data
#    - Confirmar presencia del archivo CSV
#    - Verificar URL de acceso
```

### Prueba 3: Configuración de Data Factory

```bash
# 1. Ejecutar configuración Python
python configure_azure_resources.py

# 2. Verificar en Azure Portal:
#    - Ir a Data Factory > Author & Monitor
#    - Confirmar Linked Services creados
#    - Verificar Datasets configurados
#    - Revisar Pipelines disponibles
```

### Prueba 4: Procesamiento de Eventos

```bash
# 1. Enviar eventos de prueba a Event Hub
# (Usar Azure CLI o SDK)
az eventhubs eventhub send --resource-group mvp-config-driven-pipeline-dev-rg --namespace-name tu-namespace --name data-events-hub --body "{'test': 'data'}"

# 2. Verificar en Azure Portal:
#    - Ir a Event Hub > Metrics
#    - Confirmar mensajes recibidos
#    - Revisar Consumer Groups activos
```

## 🛠️ Herramientas Adicionales

### Azure CLI

```bash
# Listar todos los recursos
az resource list --resource-group mvp-config-driven-pipeline-dev-rg --output table

# Verificar Storage Account
az storage account show --name mvpdevsa --resource-group mvp-config-driven-pipeline-dev-rg

# Verificar SQL Server
az sql server list --resource-group mvp-config-driven-pipeline-dev-rg

# Verificar Data Factory
az datafactory list --resource-group mvp-config-driven-pipeline-dev-rg
```

### Azure PowerShell

```powershell
# Obtener información de recursos
Get-AzResource -ResourceGroupName "mvp-config-driven-pipeline-dev-rg"

# Verificar métricas de Storage
Get-AzMetric -ResourceId "/subscriptions/tu-subscription/resourceGroups/mvp-config-driven-pipeline-dev-rg/providers/Microsoft.Storage/storageAccounts/mvpdevsa"

# Ejecutar pipeline de Data Factory
Invoke-AzDataFactoryV2Pipeline -ResourceGroupName "mvp-config-driven-pipeline-dev-rg" -DataFactoryName "tu-data-factory" -PipelineName "CSV-Ingestion-Pipeline"
```

### ARM Templates

```json
// Ejemplo de template para verificar configuración
{
    "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
    "contentVersion": "1.0.0.0",
    "parameters": {
        "resourceGroupName": {
            "type": "string",
            "defaultValue": "mvp-config-driven-pipeline-dev-rg"
        }
    },
    "resources": [
        // Definiciones de recursos para validación
    ]
}
```

## 📈 Monitoreo y Métricas

### Métricas Clave a Monitorear

1. **Storage Account**:
   - Transacciones por segundo
   - Latencia de requests
   - Disponibilidad del servicio

2. **SQL Database**:
   - DTU/CPU utilization
   - Conexiones activas
   - Tiempo de respuesta de queries

3. **Data Factory**:
   - Ejecuciones de pipeline exitosas/fallidas
   - Duración de actividades
   - Throughput de datos

4. **Event Hub**:
   - Mensajes entrantes/salientes
   - Throughput en bytes
   - Errores de conexión

### Configuración de Alertas

```powershell
# Crear alerta para fallos de pipeline
New-AzMetricAlertRuleV2 -Name "DataFactory-Pipeline-Failures" -ResourceGroupName "mvp-config-driven-pipeline-dev-rg" -TargetResourceId "/subscriptions/tu-subscription/resourceGroups/mvp-config-driven-pipeline-dev-rg/providers/Microsoft.DataFactory/factories/tu-data-factory" -MetricName "PipelineFailedRuns" -Operator GreaterThan -Threshold 0 -WindowSize 00:05:00 -Frequency 00:01:00
```

## 🔍 Troubleshooting

### Problemas Comunes

1. **Error de Autenticación**:
   ```powershell
   # Solución: Re-autenticar
   Connect-AzAccount
   Set-AzContext -SubscriptionId "tu-subscription-id"
   ```

2. **Permisos Insuficientes**:
   ```bash
   # Verificar roles asignados
   az role assignment list --assignee tu-usuario@dominio.com
   ```

3. **Recursos No Encontrados**:
   ```powershell
   # Verificar existencia
   Get-AzResourceGroup -Name "mvp-config-driven-pipeline-dev-rg"
   ```

4. **Fallos en Carga de Datos**:
   ```powershell
   # Verificar conectividad a Storage
   Test-AzStorageAccount -StorageAccountName "mvpdevsa"
   ```

### Logs y Diagnósticos

```bash
# Habilitar logs de diagnóstico
az monitor diagnostic-settings create --resource "/subscriptions/tu-subscription/resourceGroups/mvp-config-driven-pipeline-dev-rg/providers/Microsoft.DataFactory/factories/tu-data-factory" --name "DataFactoryDiagnostics" --logs '[{"category": "PipelineRuns", "enabled": true}]' --workspace "/subscriptions/tu-subscription/resourceGroups/mvp-config-driven-pipeline-dev-rg/providers/Microsoft.OperationalInsights/workspaces/tu-workspace"
```

## 📚 Recursos Adicionales

- [Documentación de Azure Data Factory](https://docs.microsoft.com/azure/data-factory/)
- [Guía de Azure Event Hubs](https://docs.microsoft.com/azure/event-hubs/)
- [Azure Storage Documentation](https://docs.microsoft.com/azure/storage/)
- [Azure SQL Database Docs](https://docs.microsoft.com/azure/azure-sql/)

## 🤝 Soporte

Para problemas o preguntas:

1. Revisar logs en Azure Portal
2. Consultar documentación oficial de Azure
3. Verificar configuración de permisos
4. Contactar al equipo de DevOps

---

**Nota**: Esta guía asume que los recursos de Azure ya han sido desplegados usando Terraform. Si necesitas desplegar los recursos, ejecuta primero `terraform apply` en el directorio raíz del proyecto.