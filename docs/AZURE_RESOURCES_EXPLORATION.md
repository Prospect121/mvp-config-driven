# 🔍 Guía Completa para Explorar Recursos en Azure

## 📋 Tabla de Contenidos
1. [Descripción de los Recursos Azure](#1-descripción-de-los-recursos-azure)
2. [Navegación en Azure Portal](#2-navegación-en-azure-portal)
3. [Funcionalidad y Configuración](#3-funcionalidad-y-configuración)
4. [Primera Prueba en Azure](#4-primera-prueba-en-azure)
5. [Herramientas Adicionales](#5-herramientas-adicionales)
6. [Recursos Específicos del MVP](#6-recursos-específicos-del-mvp)

---

## 1. Descripción de los Recursos Azure

### 🏗️ **Tipos de Recursos Principales**

#### **Resource Group (Grupo de Recursos)**
- **Propósito**: Contenedor lógico que agrupa recursos relacionados
- **Casos de uso**: Organización, gestión de permisos, facturación
- **Ejemplo**: `mvp-config-driven-pipeline-dev-rg`

#### **Storage Account (Cuenta de Almacenamiento)**
- **Propósito**: Almacenamiento de datos (blobs, archivos, colas, tablas)
- **Casos de uso**: Data Lake, backup, archivos estáticos
- **Tipos**: General Purpose v2, Blob Storage, File Storage
- **Ejemplo**: `mvpdevsa`

#### **SQL Server y Database**
- **Propósito**: Base de datos relacional administrada
- **Casos de uso**: Aplicaciones transaccionales, data warehousing
- **Características**: Alta disponibilidad, escalabilidad automática
- **Ejemplo**: `mvp-config-driven-pipeline-dev-sql-v2`

#### **Event Hub**
- **Propósito**: Ingesta de datos en tiempo real a gran escala
- **Casos de uso**: Streaming de eventos, telemetría, logs
- **Capacidad**: Millones de eventos por segundo
- **Ejemplo**: `mvp-config-driven-pipeline-dev-eh`

#### **Data Factory**
- **Propósito**: Servicio de integración de datos ETL/ELT
- **Casos de uso**: Pipelines de datos, transformaciones, orquestación
- **Características**: Sin servidor, escalable, visual
- **Ejemplo**: `mvp-config-driven-pipeline-dev-df`

#### **Key Vault**
- **Propósito**: Gestión segura de secretos, claves y certificados
- **Casos de uso**: Almacenar connection strings, API keys, certificados
- **Seguridad**: HSM, control de acceso granular
- **Ejemplo**: `mvp-dev-kv-v2`

#### **Log Analytics Workspace**
- **Propósito**: Recopilación y análisis de logs y métricas
- **Casos de uso**: Monitoreo, alertas, troubleshooting
- **Características**: Consultas KQL, dashboards, alertas
- **Ejemplo**: `mvp-config-driven-pipeline-dev-law`

---

## 2. Navegación en Azure Portal

### 🌐 **Acceso al Portal**
1. Navega a [portal.azure.com](https://portal.azure.com)
2. Inicia sesión con tus credenciales de Azure
3. Selecciona la suscripción correcta: `d6a71f50-d4ae-463a-9b56-e4a54988c47e`

### 🔍 **Búsqueda y Filtros**

#### **Búsqueda Global**
- **Ubicación**: Barra superior del portal
- **Funcionalidad**: Buscar recursos por nombre, tipo o etiquetas
- **Ejemplo**: Buscar "mvp-config" para encontrar todos los recursos del proyecto

#### **Filtros por Grupo de Recursos**
1. Ve a "Resource groups" en el menú lateral
2. Busca: `mvp-config-driven-pipeline-dev-rg`
3. Haz clic para ver todos los recursos del proyecto

#### **Filtros Avanzados**
- **Por tipo de recurso**: Storage accounts, SQL databases, etc.
- **Por ubicación**: West US 2
- **Por etiquetas**: Environment=dev, Project=mvp-config-driven-pipeline

### 📊 **Vista de Dashboard**
- **All resources**: Vista completa de todos los recursos
- **Resource groups**: Vista organizada por grupos
- **Favorites**: Recursos marcados como favoritos
- **Recently accessed**: Recursos visitados recientemente

---

## 3. Funcionalidad y Configuración

### 🔧 **Interacción con Recursos**

#### **Storage Account (`mvpdevsa`)**
**Propiedades principales:**
- **Performance**: Standard
- **Replication**: LRS (Locally Redundant Storage)
- **Access tier**: Hot
- **Hierarchical namespace**: Habilitado (Data Lake Gen2)

**Configuraciones importantes:**
- **Containers**: Para almacenar blobs organizados
- **File shares**: Para compartir archivos
- **Access keys**: Para autenticación programática
- **SAS tokens**: Para acceso temporal y granular

**Métricas clave:**
- **Storage used**: Espacio utilizado
- **Transactions**: Número de operaciones
- **Availability**: Tiempo de actividad
- **Latency**: Tiempo de respuesta

#### **SQL Database (`mvp-config-driven-pipeline-dev-db`)**
**Propiedades principales:**
- **Service tier**: Basic/Standard/Premium
- **Compute size**: DTUs o vCores
- **Storage**: Tamaño máximo de la base de datos

**Configuraciones importantes:**
- **Connection strings**: Para conectar aplicaciones
- **Firewall rules**: Control de acceso por IP
- **Backup retention**: Política de respaldos
- **Transparent Data Encryption**: Cifrado en reposo

**Métricas clave:**
- **DTU percentage**: Utilización de recursos
- **Storage percentage**: Uso del almacenamiento
- **Connection count**: Conexiones activas
- **Query performance**: Rendimiento de consultas

#### **Event Hub (`mvp-config-driven-pipeline-dev-eh`)**
**Propiedades principales:**
- **Throughput units**: Capacidad de procesamiento
- **Partition count**: Paralelismo de procesamiento
- **Message retention**: Tiempo de retención de mensajes

**Configuraciones importantes:**
- **Connection strings**: Para productores y consumidores
- **Consumer groups**: Para múltiples lectores
- **Capture**: Para archivar eventos automáticamente

**Métricas clave:**
- **Incoming messages**: Mensajes recibidos
- **Outgoing messages**: Mensajes enviados
- **Throttled requests**: Solicitudes limitadas
- **Successful requests**: Operaciones exitosas

#### **Data Factory (`mvp-config-driven-pipeline-dev-df`)**
**Propiedades principales:**
- **Version**: V2 (actual)
- **Location**: West US 2
- **Managed Identity**: Para autenticación sin credenciales

**Configuraciones importantes:**
- **Linked services**: Conexiones a fuentes de datos
- **Datasets**: Definiciones de estructura de datos
- **Pipelines**: Flujos de trabajo de datos
- **Triggers**: Programación de ejecuciones

**Métricas clave:**
- **Pipeline runs**: Ejecuciones de pipelines
- **Activity runs**: Ejecuciones de actividades
- **Trigger runs**: Activaciones de triggers
- **Data movement**: Volumen de datos procesados

---

## 4. Primera Prueba en Azure

### 🧪 **Prueba Básica con CSV**

#### **Paso 1: Preparar el archivo CSV de prueba**
```csv
id,name,category,price,timestamp
1,Product A,Electronics,299.99,2025-01-20T10:00:00Z
2,Product B,Clothing,49.99,2025-01-20T10:01:00Z
3,Product C,Books,19.99,2025-01-20T10:02:00Z
4,Product D,Electronics,599.99,2025-01-20T10:03:00Z
5,Product E,Home,89.99,2025-01-20T10:04:00Z
```

#### **Paso 2: Subir archivo a Storage Account**
1. **Acceder al Storage Account**:
   - Ve a Azure Portal → Storage accounts → `mvpdevsa`
   
2. **Crear container**:
   - Ve a "Containers" → "+ Container"
   - Nombre: `test-data`
   - Public access level: Private
   - Haz clic en "Create"

3. **Subir archivo**:
   - Entra al container `test-data`
   - Haz clic en "Upload"
   - Selecciona tu archivo CSV
   - Haz clic en "Upload"

#### **Paso 3: Configurar Data Factory Pipeline**
1. **Acceder a Data Factory**:
   - Ve a Azure Portal → Data factories → `mvp-config-driven-pipeline-dev-df`
   - Haz clic en "Open Azure Data Factory Studio"

2. **Crear Linked Service para Storage**:
   ```json
   {
     "name": "StorageLinkedService",
     "type": "AzureBlobStorage",
     "properties": {
       "connectionString": "DefaultEndpointsProtocol=https;AccountName=mvpdevsa;AccountKey=<key>;EndpointSuffix=core.windows.net"
     }
   }
   ```

3. **Crear Dataset para CSV**:
   ```json
   {
     "name": "CsvDataset",
     "type": "DelimitedText",
     "properties": {
       "linkedServiceName": "StorageLinkedService",
       "location": {
         "type": "AzureBlobStorageLocation",
         "container": "test-data",
         "fileName": "test-products.csv"
       },
       "columnDelimiter": ",",
       "firstRowAsHeader": true
     }
   }
   ```

4. **Crear Pipeline simple**:
   ```json
   {
     "name": "TestCsvPipeline",
     "activities": [
       {
         "name": "CopyData",
         "type": "Copy",
         "inputs": [{"referenceName": "CsvDataset"}],
         "outputs": [{"referenceName": "SqlDataset"}],
         "source": {"type": "DelimitedTextSource"},
         "sink": {"type": "AzureSqlSink"}
       }
     ]
   }
   ```

#### **Paso 4: Configurar SQL Database**
1. **Crear tabla de destino**:
   ```sql
   CREATE TABLE test_products (
       id INT PRIMARY KEY,
       name NVARCHAR(100),
       category NVARCHAR(50),
       price DECIMAL(10,2),
       timestamp DATETIME2
   );
   ```

2. **Verificar conexión**:
   - Usa SQL Server Management Studio o Azure Data Studio
   - Connection string disponible en Key Vault

#### **Paso 5: Ejecutar y monitorear**
1. **Ejecutar pipeline**:
   - En Data Factory Studio → Pipelines → TestCsvPipeline
   - Haz clic en "Debug" o "Trigger now"

2. **Monitorear ejecución**:
   - Ve a "Monitor" → "Pipeline runs"
   - Verifica el estado y logs de ejecución

3. **Verificar resultados**:
   ```sql
   SELECT * FROM test_products;
   ```

### 📁 **Implementar Archivos de Configuración**

#### **Archivo de configuración para Data Factory**
```yaml
# config/data_factory_config.yml
pipelines:
  csv_ingestion:
    source:
      type: "DelimitedText"
      location: "test-data/test-products.csv"
      format:
        delimiter: ","
        header: true
    destination:
      type: "AzureSqlDatabase"
      table: "test_products"
      write_mode: "append"
    
  monitoring:
    alerts:
      - type: "failure"
        notification: "email"
      - type: "success"
        notification: "log"
```

#### **Archivo de configuración para Event Hub**
```yaml
# config/eventhub_config.yml
event_hubs:
  transaction_events:
    partition_count: 4
    retention_days: 7
    consumer_groups:
      - "data_factory_consumer"
      - "analytics_consumer"
  
  telemetry_events:
    partition_count: 2
    retention_days: 1
    consumer_groups:
      - "monitoring_consumer"
```

#### **Script de configuración automatizada**
```python
# scripts/configure_azure_resources.py
import yaml
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

def configure_data_factory():
    """Configura Data Factory basado en archivo de configuración"""
    
    # Cargar configuración
    with open('config/data_factory_config.yml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Inicializar cliente
    credential = DefaultAzureCredential()
    df_client = DataFactoryManagementClient(
        credential, 
        "d6a71f50-d4ae-463a-9b56-e4a54988c47e"
    )
    
    # Crear pipelines basados en configuración
    for pipeline_name, pipeline_config in config['pipelines'].items():
        print(f"Configurando pipeline: {pipeline_name}")
        # Implementar lógica de creación de pipeline
        
if __name__ == "__main__":
    configure_data_factory()
```

---

## 5. Herramientas Adicionales

### 🔧 **Azure CLI**

#### **Instalación**
```bash
# Windows
winget install Microsoft.AzureCLI

# macOS
brew install azure-cli

# Linux
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

#### **Comandos útiles para nuestros recursos**
```bash
# Autenticación
az login

# Listar recursos del grupo
az resource list --resource-group mvp-config-driven-pipeline-dev-rg

# Información del Storage Account
az storage account show --name mvpdevsa --resource-group mvp-config-driven-pipeline-dev-rg

# Información de SQL Database
az sql db show --server mvp-config-driven-pipeline-dev-sql-v2 --name mvp-config-driven-pipeline-dev-db --resource-group mvp-config-driven-pipeline-dev-rg

# Listar Event Hubs
az eventhubs eventhub list --namespace-name mvp-config-driven-pipeline-dev-eh --resource-group mvp-config-driven-pipeline-dev-rg

# Información de Data Factory
az datafactory show --name mvp-config-driven-pipeline-dev-df --resource-group mvp-config-driven-pipeline-dev-rg

# Secretos de Key Vault
az keyvault secret list --vault-name mvp-dev-kv-v2
```

### 💻 **Azure PowerShell**

#### **Instalación**
```powershell
Install-Module -Name Az -AllowClobber -Scope CurrentUser
```

#### **Comandos útiles**
```powershell
# Autenticación
Connect-AzAccount

# Seleccionar suscripción
Select-AzSubscription -SubscriptionId "d6a71f50-d4ae-463a-9b56-e4a54988c47e"

# Obtener recursos del grupo
Get-AzResource -ResourceGroupName "mvp-config-driven-pipeline-dev-rg"

# Información de Storage Account
Get-AzStorageAccount -ResourceGroupName "mvp-config-driven-pipeline-dev-rg" -Name "mvpdevsa"

# Métricas de SQL Database
Get-AzSqlDatabase -ServerName "mvp-config-driven-pipeline-dev-sql-v2" -ResourceGroupName "mvp-config-driven-pipeline-dev-rg"

# Pipelines de Data Factory
Get-AzDataFactoryV2Pipeline -ResourceGroupName "mvp-config-driven-pipeline-dev-rg" -DataFactoryName "mvp-config-driven-pipeline-dev-df"
```

### 🏗️ **Azure Resource Manager (ARM)**

#### **Template para recrear recursos**
```json
{
  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "environment": {
      "type": "string",
      "defaultValue": "dev"
    }
  },
  "resources": [
    {
      "type": "Microsoft.Storage/storageAccounts",
      "apiVersion": "2021-04-01",
      "name": "[concat('mvp', parameters('environment'), 'sa')]",
      "location": "westus2",
      "sku": {
        "name": "Standard_LRS"
      },
      "kind": "StorageV2",
      "properties": {
        "isHnsEnabled": true
      }
    }
  ]
}
```

#### **Bicep (alternativa moderna a ARM)**
```bicep
param environment string = 'dev'
param location string = resourceGroup().location

resource storageAccount 'Microsoft.Storage/storageAccounts@2021-04-01' = {
  name: 'mvp${environment}sa'
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
  }
}
```

### 🐍 **SDKs de Azure para Python**

#### **Instalación**
```bash
pip install azure-identity azure-mgmt-storage azure-mgmt-sql azure-mgmt-datafactory
```

#### **Ejemplo de uso**
```python
from azure.identity import DefaultAzureCredential
from azure.mgmt.storage import StorageManagementClient

# Autenticación
credential = DefaultAzureCredential()

# Cliente de Storage
storage_client = StorageManagementClient(
    credential, 
    "d6a71f50-d4ae-463a-9b56-e4a54988c47e"
)

# Obtener información de Storage Account
storage_account = storage_client.storage_accounts.get_properties(
    "mvp-config-driven-pipeline-dev-rg", 
    "mvpdevsa"
)

print(f"Storage Account: {storage_account.name}")
print(f"Location: {storage_account.location}")
print(f"SKU: {storage_account.sku.name}")
```

---

## 6. Recursos Específicos del MVP

### 📊 **Resumen de Recursos Desplegados**

| Recurso | Nombre | Propósito | Estado |
|---------|--------|-----------|--------|
| Resource Group | `mvp-config-driven-pipeline-dev-rg` | Contenedor de recursos | ✅ Activo |
| Storage Account | `mvpdevsa` | Data Lake Gen2 | ✅ Activo |
| SQL Server | `mvp-config-driven-pipeline-dev-sql-v2` | Base de datos | ✅ Activo |
| SQL Database | `mvp-config-driven-pipeline-dev-db` | Almacén de datos | ✅ Activo |
| Event Hub Namespace | `mvp-config-driven-pipeline-dev-eh` | Streaming de eventos | ✅ Activo |
| Data Factory | `mvp-config-driven-pipeline-dev-df` | Orquestación ETL | ✅ Activo |
| Key Vault | `mvp-dev-kv-v2` | Gestión de secretos | ✅ Activo |
| Log Analytics | `mvp-config-driven-pipeline-dev-law` | Monitoreo y logs | ✅ Activo |

### 🔐 **Configuración de Seguridad RBAC**

| Servicio | Rol Asignado | Principal | Propósito |
|----------|--------------|-----------|-----------|
| Key Vault | Key Vault Secrets User | Data Factory | Acceso a secretos |
| SQL Database | SQL DB Contributor | Data Factory | Lectura/escritura de datos |
| Event Hub | Azure Event Hubs Data Owner | Data Factory | Envío/recepción de eventos |
| Storage Account | Storage Blob Data Contributor | Data Factory | Acceso a blobs |

### 📈 **Métricas y Monitoreo**

#### **Dashboards recomendados**
1. **Data Pipeline Health**: Estado de pipelines de Data Factory
2. **Storage Performance**: Métricas de Storage Account
3. **Database Performance**: Rendimiento de SQL Database
4. **Event Hub Throughput**: Volumen de eventos procesados

#### **Alertas configuradas**
- **Pipeline failures**: Fallos en Data Factory
- **Storage capacity**: Uso de almacenamiento > 80%
- **Database DTU**: Utilización > 90%
- **Event Hub throttling**: Solicitudes limitadas

---

## 🎯 **Próximos Pasos Recomendados**

1. **Explorar Azure Portal**: Familiarízate con la interfaz navegando por cada recurso
2. **Ejecutar prueba CSV**: Sigue el paso a paso para procesar tu primer archivo
3. **Configurar monitoreo**: Establece alertas personalizadas para tu caso de uso
4. **Implementar seguridad**: Revisa y ajusta permisos según tus necesidades
5. **Automatizar despliegues**: Usa Terraform o ARM templates para entornos adicionales

---

## 📚 **Recursos Adicionales**

- [Documentación oficial de Azure](https://docs.microsoft.com/azure/)
- [Azure Architecture Center](https://docs.microsoft.com/azure/architecture/)
- [Azure CLI Reference](https://docs.microsoft.com/cli/azure/)
- [Azure PowerShell Documentation](https://docs.microsoft.com/powershell/azure/)
- [Azure SDK for Python](https://docs.microsoft.com/python/api/overview/azure/)

---

*Documento generado para el proyecto MVP Config-Driven Pipeline - Enero 2025*