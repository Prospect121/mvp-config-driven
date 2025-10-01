# Guía de Despliegue - MVP Config-Driven Data Pipeline

## Índice
1. [Requisitos Previos](#requisitos-previos)
2. [Despliegue Local](#despliegue-local)
3. [Despliegue en Azure](#despliegue-en-azure)
4. [Configuración de Entornos](#configuración-de-entornos)
5. [Monitoreo y Observabilidad](#monitoreo-y-observabilidad)
6. [Troubleshooting](#troubleshooting)
7. [Mantenimiento](#mantenimiento)

## Requisitos Previos

### Herramientas Necesarias

#### Para Desarrollo Local
- **Docker Desktop** (v4.0+)
- **Docker Compose** (v2.0+)
- **Python** (3.9+)
- **Git**
- **PowerShell** (Windows) o **Bash** (Linux/macOS)

#### Para Despliegue en Azure
- **Azure CLI** (v2.40+)
- **Terraform** (v1.0+)
- **Azure PowerShell** (opcional)
- **Cuenta de Azure** con permisos de Contributor

### Verificación de Requisitos

```bash
# Verificar Docker
docker --version
docker-compose --version

# Verificar Azure CLI
az --version
az account show

# Verificar Terraform
terraform --version

# Verificar Python
python --version
pip --version
```

## Despliegue Local

### 1. Configuración Inicial

```bash
# Clonar el repositorio
git clone <repository-url>
cd mvp-config-driven

# Crear archivo de variables de entorno
cp .env.example .env

# Editar variables según tu entorno
# Nota: Los valores por defecto están configurados para desarrollo local
```

### 2. Configuración de Variables de Entorno

Editar el archivo `.env`:

```bash
# === CONFIGURACIÓN LOCAL ===
# Base de datos SQL Server
SQL_SERVER_SA_PASSWORD=YourStrong@Passw0rd
SQL_SERVER_DATABASE=mvp_config_driven

# MinIO (S3 compatible)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_ENDPOINT=localhost:9000

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_TELEMETRY=telemetry-events
KAFKA_TOPIC_TRANSACTIONS=transaction-events

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=redis123

# Spark
SPARK_MASTER_URL=spark://localhost:7077
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g

# Monitoreo
GRAFANA_ADMIN_PASSWORD=admin123
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000

# Jupyter
JUPYTER_TOKEN=your-jupyter-token
JUPYTER_PORT=8888

# Kafka UI
KAFKA_UI_PORT=8080

# === CONFIGURACIÓN DE SEGURIDAD LOCAL ===
ENCRYPTION_KEY=your-local-encryption-key-32-chars
JWT_SECRET=your-local-jwt-secret
AUDIT_LEVEL=INFO
```

### 3. Ejecutar el Entorno Local

#### Windows (PowerShell)
```powershell
# Ejecutar script de configuración
.\scripts\setup_local_environment.ps1

# O manualmente paso a paso:
# 1. Verificar Docker
if (Get-Command docker -ErrorAction SilentlyContinue) {
    Write-Host "✅ Docker está instalado"
} else {
    Write-Host "❌ Docker no está instalado"
    exit 1
}

# 2. Crear directorios de datos
New-Item -ItemType Directory -Force -Path ".\data\minio"
New-Item -ItemType Directory -Force -Path ".\data\sql"
New-Item -ItemType Directory -Force -Path ".\data\kafka"
New-Item -ItemType Directory -Force -Path ".\data\redis"
New-Item -ItemType Directory -Force -Path ".\data\spark"
New-Item -ItemType Directory -Force -Path ".\data\prometheus"
New-Item -ItemType Directory -Force -Path ".\data\grafana"

# 3. Iniciar servicios de infraestructura
docker-compose up -d zookeeper redis sqlserver minio

# 4. Esperar a que SQL Server esté listo
Write-Host "Esperando a que SQL Server esté listo..."
do {
    Start-Sleep -Seconds 5
    $result = docker exec mvp-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $env:SQL_SERVER_SA_PASSWORD -Q "SELECT 1" 2>$null
} while ($LASTEXITCODE -ne 0)

# 5. Iniciar Kafka
docker-compose up -d kafka

# 6. Crear topics de Kafka
Start-Sleep -Seconds 10
docker exec mvp-kafka kafka-topics.sh --create --topic telemetry-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec mvp-kafka kafka-topics.sh --create --topic transaction-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 7. Configurar buckets de MinIO
# (Se configura automáticamente en el contenedor)

# 8. Iniciar servicios de Spark
docker-compose up -d spark-master spark-worker

# 9. Iniciar servicios de monitoreo
docker-compose up -d prometheus grafana

# 10. Iniciar servicios adicionales
docker-compose up -d jupyter kafka-ui
```

#### Linux/macOS (Bash)
```bash
# Ejecutar script de configuración
chmod +x scripts/setup_local_environment.sh
./scripts/setup_local_environment.sh
```

### 4. Verificar el Despliegue Local

```bash
# Verificar que todos los contenedores estén ejecutándose
docker-compose ps

# Verificar logs si hay problemas
docker-compose logs [service-name]

# Probar conectividad
curl http://localhost:9000  # MinIO
curl http://localhost:3000  # Grafana
curl http://localhost:9090  # Prometheus
curl http://localhost:8080  # Kafka UI
curl http://localhost:8888  # Jupyter
```

### 5. Acceder a los Servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin123 |
| **Grafana** | http://localhost:3000 | admin / admin123 |
| **Prometheus** | http://localhost:9090 | - |
| **Kafka UI** | http://localhost:8080 | - |
| **Jupyter** | http://localhost:8888 | Token: your-jupyter-token |
| **Spark Master** | http://localhost:8080 | - |
| **SQL Server** | localhost:1433 | sa / YourStrong@Passw0rd |

## Despliegue en Azure

### 1. Configuración de Azure

```bash
# Iniciar sesión en Azure
az login

# Seleccionar suscripción
az account set --subscription "your-subscription-id"

# Crear service principal para Terraform (opcional)
az ad sp create-for-rbac --name "terraform-sp" --role="Contributor" --scopes="/subscriptions/your-subscription-id"
```

### 2. Configuración de Terraform

```bash
# Navegar al directorio de Terraform
cd terraform

# Crear archivo de variables
cp terraform.tfvars.example terraform.tfvars
```

Editar `terraform.tfvars`:

```hcl
# === CONFIGURACIÓN BÁSICA ===
project_name = "mvp-config-driven"
environment = "prod"  # o "dev", "staging"
location = "East US 2"

# === CONFIGURACIÓN DE RED ===
vnet_address_space = ["10.0.0.0/16"]
subnet_address_prefixes = ["10.0.1.0/24", "10.0.2.0/24"]

# === CONFIGURACIÓN DE SEGURIDAD ===
data_engineers_group_name = "DataEngineers"
data_scientists_group_name = "DataScientists"
enable_rbac_audit = true
enable_azure_policies = true
enable_mfa_for_admin = true

# === CONFIGURACIÓN DE BACKUP ===
sql_backup_retention_days = 35
sql_geo_redundant_backup_enabled = true

# === IPs CONFIABLES ===
trusted_ip_ranges = [
  "203.0.113.0/24",  # Oficina principal
  "198.51.100.0/24"  # Oficina secundaria
]

# === CONFIGURACIÓN DE MONITOREO ===
log_analytics_retention_days = 90
enable_application_insights = true

# === CONFIGURACIÓN DE RECURSOS ===
sql_sku_name = "S2"  # Standard S2
storage_account_tier = "Standard"
storage_account_replication_type = "GRS"
```

### 3. Desplegar Infraestructura

```bash
# Inicializar Terraform
terraform init

# Planificar el despliegue
terraform plan -var-file="terraform.tfvars"

# Aplicar el despliegue
terraform apply -var-file="terraform.tfvars"

# Guardar outputs importantes
terraform output > ../deployment-outputs.txt
```

### 4. Configurar Secretos en Key Vault

```bash
# Obtener el nombre del Key Vault desde los outputs
KEY_VAULT_NAME=$(terraform output -raw key_vault_name)

# Configurar secretos necesarios
az keyvault secret set --vault-name $KEY_VAULT_NAME --name "sql-admin-password" --value "YourSecurePassword123!"
az keyvault secret set --vault-name $KEY_VAULT_NAME --name "storage-connection-string" --value "$(terraform output -raw storage_connection_string)"
az keyvault secret set --vault-name $KEY_VAULT_NAME --name "event-hub-connection-string" --value "$(terraform output -raw event_hub_connection_string)"
az keyvault secret set --vault-name $KEY_VAULT_NAME --name "application-insights-key" --value "$(terraform output -raw application_insights_instrumentation_key)"

# Secretos de aplicación
az keyvault secret set --vault-name $KEY_VAULT_NAME --name "encryption-key" --value "$(openssl rand -base64 32)"
az keyvault secret set --vault-name $KEY_VAULT_NAME --name "jwt-secret" --value "$(openssl rand -base64 64)"
```

### 5. Configurar Data Factory

```bash
# Crear pipelines desde plantillas
RESOURCE_GROUP=$(terraform output -raw resource_group_name)
DATA_FACTORY_NAME=$(terraform output -raw data_factory_name)

# Desplegar pipelines usando Azure CLI
az datafactory pipeline create \
  --resource-group $RESOURCE_GROUP \
  --factory-name $DATA_FACTORY_NAME \
  --name "ingestion-pipeline" \
  --pipeline @pipelines/ingestion-pipeline.json

az datafactory pipeline create \
  --resource-group $RESOURCE_GROUP \
  --factory-name $DATA_FACTORY_NAME \
  --name "transformation-pipeline" \
  --pipeline @pipelines/transformation-pipeline.json
```

### 6. Configurar Monitoreo

```bash
# Importar dashboards de Grafana
GRAFANA_URL=$(terraform output -raw grafana_url)
GRAFANA_API_KEY=$(az keyvault secret show --vault-name $KEY_VAULT_NAME --name "grafana-api-key" --query value -o tsv)

# Importar dashboard principal
curl -X POST \
  -H "Authorization: Bearer $GRAFANA_API_KEY" \
  -H "Content-Type: application/json" \
  -d @monitoring/grafana-dashboard.json \
  $GRAFANA_URL/api/dashboards/db
```

## Configuración de Entornos

### Desarrollo (dev)

```hcl
# terraform/environments/dev.tfvars
environment = "dev"
sql_sku_name = "Basic"
storage_account_replication_type = "LRS"
enable_azure_policies = false
log_analytics_retention_days = 30
```

### Staging (staging)

```hcl
# terraform/environments/staging.tfvars
environment = "staging"
sql_sku_name = "S1"
storage_account_replication_type = "GRS"
enable_azure_policies = true
log_analytics_retention_days = 60
```

### Producción (prod)

```hcl
# terraform/environments/prod.tfvars
environment = "prod"
sql_sku_name = "S2"
storage_account_replication_type = "GRS"
enable_azure_policies = true
enable_mfa_for_admin = true
sql_backup_retention_days = 35
log_analytics_retention_days = 90
```

### Despliegue por Entorno

```bash
# Desarrollo
terraform workspace new dev
terraform apply -var-file="environments/dev.tfvars"

# Staging
terraform workspace new staging
terraform apply -var-file="environments/staging.tfvars"

# Producción
terraform workspace new prod
terraform apply -var-file="environments/prod.tfvars"
```

## Monitoreo y Observabilidad

### Configurar Application Insights

```python
# src/main.py
from src.utils.azure_integration import initialize_azure_services

# Inicializar servicios de Azure (incluyendo Application Insights)
initialize_azure_services()

# El logging automáticamente enviará telemetría a Application Insights
```

### Dashboards de Grafana

Los dashboards se configuran automáticamente e incluyen:

- **Pipeline Performance**: Métricas de rendimiento de pipelines
- **Data Quality**: Métricas de calidad de datos
- **Security Audit**: Eventos de seguridad y acceso
- **Infrastructure**: Métricas de infraestructura (CPU, memoria, red)

### Alertas

```bash
# Configurar alertas en Azure Monitor
az monitor metrics alert create \
  --name "High Error Rate" \
  --resource-group $RESOURCE_GROUP \
  --scopes $(terraform output -raw data_factory_id) \
  --condition "avg exceptions/requests > 0.1" \
  --description "Alert when error rate exceeds 10%"
```

## Troubleshooting

### Problemas Comunes en Local

#### Docker no inicia
```bash
# Verificar estado de Docker
docker info

# Reiniciar Docker Desktop
# Windows: Restart Docker Desktop
# Linux: sudo systemctl restart docker
```

#### SQL Server no se conecta
```bash
# Verificar logs
docker logs mvp-sqlserver

# Verificar conectividad
docker exec mvp-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $SQL_SERVER_SA_PASSWORD -Q "SELECT 1"
```

#### Kafka no crea topics
```bash
# Verificar logs de Kafka
docker logs mvp-kafka

# Crear topics manualmente
docker exec mvp-kafka kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092
```

### Problemas Comunes en Azure

#### Terraform falla en apply
```bash
# Verificar permisos
az account show
az role assignment list --assignee $(az account show --query user.name -o tsv)

# Verificar estado de Terraform
terraform state list
terraform refresh
```

#### Key Vault acceso denegado
```bash
# Verificar permisos en Key Vault
az keyvault show --name $KEY_VAULT_NAME --query properties.accessPolicies

# Agregar política de acceso
az keyvault set-policy --name $KEY_VAULT_NAME --upn $(az account show --query user.name -o tsv) --secret-permissions get list set
```

#### Data Factory pipeline falla
```bash
# Verificar logs en Azure Portal
az datafactory pipeline-run query-by-factory \
  --resource-group $RESOURCE_GROUP \
  --factory-name $DATA_FACTORY_NAME \
  --last-updated-after "2023-01-01" \
  --last-updated-before "2023-12-31"
```

### Logs y Diagnósticos

```bash
# Logs locales
docker-compose logs -f [service-name]

# Logs de Azure
az monitor activity-log list --resource-group $RESOURCE_GROUP
az monitor diagnostic-settings list --resource $(terraform output -raw data_factory_id)
```

## Mantenimiento

### Actualizaciones Regulares

#### Semanal
- Revisar logs de seguridad
- Verificar métricas de rendimiento
- Actualizar dependencias de Python

#### Mensual
- Actualizar imágenes de Docker
- Revisar y rotar secretos
- Ejecutar pruebas de backup/restore

#### Trimestral
- Actualizar versiones de Terraform
- Revisar políticas de seguridad
- Auditoría de accesos y permisos

### Scripts de Mantenimiento

```bash
# scripts/maintenance.sh
#!/bin/bash

# Actualizar imágenes Docker
docker-compose pull

# Limpiar imágenes no utilizadas
docker image prune -f

# Backup de configuración
tar -czf backup-$(date +%Y%m%d).tar.gz terraform/ docker/ scripts/

# Verificar estado de servicios
docker-compose ps
```

### Backup y Restore

#### Backup Local
```bash
# Backup de datos
docker exec mvp-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $SQL_SERVER_SA_PASSWORD -Q "BACKUP DATABASE mvp_config_driven TO DISK = '/var/opt/mssql/backup/mvp_backup.bak'"

# Backup de MinIO
docker exec mvp-minio mc mirror /data /backup
```

#### Backup Azure
```bash
# Los backups automáticos están configurados en Terraform
# Backup manual si es necesario
az sql db export \
  --resource-group $RESOURCE_GROUP \
  --server $SQL_SERVER_NAME \
  --name $DATABASE_NAME \
  --storage-key-type StorageAccessKey \
  --storage-key $STORAGE_KEY \
  --storage-uri "https://$STORAGE_ACCOUNT.blob.core.windows.net/backups/manual-backup-$(date +%Y%m%d).bacpac"
```

### Monitoreo de Costos

```bash
# Verificar costos actuales
az consumption usage list --start-date $(date -d '30 days ago' +%Y-%m-%d) --end-date $(date +%Y-%m-%d)

# Configurar alertas de presupuesto
az consumption budget create \
  --budget-name "monthly-budget" \
  --amount 1000 \
  --time-grain Monthly \
  --time-period start-date=$(date +%Y-%m-01) \
  --notifications amount=80 operator=GreaterThan contact-emails=admin@company.com
```

---

## Recursos Adicionales

- [Docker Compose Reference](https://docs.docker.com/compose/)
- [Terraform Azure Provider](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs)
- [Azure Data Factory Documentation](https://docs.microsoft.com/en-us/azure/data-factory/)
- [Azure Monitor Documentation](https://docs.microsoft.com/en-us/azure/azure-monitor/)

Para soporte técnico, contactar: devops@company.com