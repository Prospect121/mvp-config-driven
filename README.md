# MVP Config Driven Data Pipeline

Este proyecto implementa un **pipeline de datos dinámico y flexible**, basado en **Apache Spark + MinIO + configuración YAML/JSON**, diseñado para adaptarse a distintos entornos (local, Docker, WSL2, CI/CD, Azure) y soportar cambios en datasets sin necesidad de modificar código.

## 🎯 Objetivos del proyecto

- Procesar datos en **CSV, JSON y Parquet** de forma **configurable**
- Definir **esquemas, estándares y reglas de calidad** en archivos de configuración
- Adaptarse a cambios de columnas, tipos de datos y flujos mediante **metadata-driven pipelines**
- Estandarizar datasets en capas (`raw → silver`)
- **Seguridad empresarial** con Azure AD, RBAC, cifrado y auditoría
- **Observabilidad completa** con logs estructurados, métricas y trazabilidad
- **Patrones de software** robustos (Observer, Strategy) para escalabilidad
- Ser portable y ejecutable en:
  - **Windows** (con WSL2 + Docker Desktop)
  - **Linux** nativo
  - **Azure Cloud** (Data Factory, Synapse, Key Vault)
  - **Entornos de CI/CD**

## 🏗️ Características principales

### ✅ Funcionalidades implementadas
- [x] Ingesta dinámica de CSV/JSON/Parquet
- [x] Estandarización configurable (renames, casts, defaults, deduplicación)
- [x] Enriquecimiento automático (timestamp, run_id, particiones por fecha)
- [x] **Seguridad empresarial** con Azure Key Vault, RBAC y cifrado
- [x] **Logs estructurados** con correlación y métricas
- [x] **Patrones de software** (Observer para eventos, Strategy para procesamiento)
- [x] **Pruebas unitarias** completas (>95% cobertura)
- [x] **Integración Azure** (Storage, Event Hub, Key Vault, Monitor)
- [x] **Documentación de seguridad** y despliegue
- [x] CI/CD con validación de configs
- [x] **Pipeline funcional** con datos de prueba procesados
- [x] **Monitoreo y observabilidad** validados
- [x] **Configuración Azure** lista para despliegue

## 📊 Resultados de Pruebas

### Pipeline de Datos
- ✅ **10 registros procesados** → **7 registros válidos** (70% tasa de éxito)
- ✅ **Calidad de datos**: 100% (todos los registros válidos pasaron las validaciones)
- ✅ **6 países procesados**, **3 monedas diferentes**
- ✅ **Monto total**: $817.234 USD, **promedio**: $116.75 USD

### Monitoreo y Observabilidad
- ✅ **Sistema de logging** funcionando correctamente
- ✅ **Métricas de pipeline** recolectadas y almacenadas
- ✅ **Health checks** pasando todas las validaciones
- ✅ **Prometheus y Grafana** configurados y operativos

### Infraestructura
- ✅ **Docker Compose** con 9 servicios ejecutándose
- ✅ **Spark cluster** (master + worker) operativo
- ✅ **MinIO** para almacenamiento S3-compatible
- ✅ **Kafka + Zookeeper** para streaming
- ✅ **Redis** para caché y estado
- ✅ **SQL Server** para metadatos

### 🔄 Próximas funcionalidades
- [ ] Capa `gold` y orquestación con Azure Data Factory
- [ ] Dashboard de monitoreo con Power BI
- [ ] ML Pipeline con Azure ML

---

## 🏛️ Arquitectura

```
mvp-config-driven/
├─ config/                   # Configuraciones dinámicas
│   ├─ datasets/
│   │   └─ finanzas/
│   │       └─ payments_v1/
│   │           ├─ schema.json           # Esquema de datos
│   │           ├─ expectations.yml      # Reglas de calidad
│   │           └─ pipeline.yml          # Configuración del pipeline
│   └─ envs/
│       ├─ local.yml                     # Entorno local
│       ├─ dev.yml                       # Entorno desarrollo
│       └─ prod.yml                      # Entorno producción
├─ src/                      # Código fuente
│   ├─ utils/
│   │   ├─ logging.py                    # Logs estructurados
│   │   ├─ azure_integration.py          # Integración Azure
│   │   └─ security.py                   # Utilidades de seguridad
│   └─ patterns/
│       ├─ observer.py                   # Patrón Observer
│       └─ strategy.py                   # Patrón Strategy
├─ terraform/                # Infraestructura como código
│   ├─ main.tf                          # Recursos principales
│   ├─ variables.tf                     # Variables
│   ├─ security.tf                      # Configuración de seguridad
│   └─ outputs.tf                       # Salidas
├─ tests/                    # Pruebas unitarias
│   ├─ test_logging.py
│   ├─ test_patterns.py
│   ├─ test_security.py
│   └─ test_azure_integration.py
├─ docs/                     # Documentación
│   ├─ SECURITY.md                      # Guía de seguridad
│   └─ DEPLOYMENT.md                    # Guía de despliegue
├─ pipelines/
│   └─ spark_job.py          # Pipeline principal
├─ scripts/
│   ├─ run_pipeline.ps1      # Script Windows
│   └─ runner.sh             # Script Linux
├─ ci/                       # CI/CD
│   ├─ check_config.sh
│   ├─ lint.yml
│   ├─ test_dataset.yml
│   └─ README.md
├─ docker-compose.yml        # Orquestación local
├─ Makefile                  # Comandos automatizados
├─ requirements.txt          # Dependencias Python
└─ README.md                 
```

### 🔧 Servicios principales

| Servicio | Rol | Entorno |
|----------|-----|---------|
| **Spark Master/Worker** | Motor de procesamiento distribuido | Local/Azure |
| **MinIO/Azure Storage** | Almacenamiento para raw/silver/quarantine | Local/Azure |
| **Runner** | Ejecutor de pipelines con configs dinámicas | Local/Azure |
| **Azure Key Vault** | Gestión segura de secretos y certificados | Azure |
| **Azure Event Hub** | Streaming de eventos y telemetría | Azure |
| **Azure Monitor** | Observabilidad y alertas | Azure |
| **Application Gateway + WAF** | Seguridad en tránsito y protección | Azure |

---

## 📋 Instalación y requisitos

### 🔧 Requisitos previos

#### Para desarrollo local:
- **Docker Desktop** (Windows) o **Docker + Docker Compose** (Linux)
- **Python 3.9+** con pip
- **Git**
- **Make** (opcional, para comandos automatizados)

#### Para despliegue en Azure:
- **Azure CLI** (`az`)
- **Terraform** (>= 1.0)
- **Suscripción Azure** con permisos de Contributor
- **Azure AD** con permisos para crear Service Principals

### 🪟 Windows (con WSL2 + Docker Desktop)

1. **Instalar Docker Desktop** y habilitar integración con WSL2
2. **Clonar el repositorio:**

```bash
git clone https://github.com/mi-org/mvp-config-driven.git
cd mvp-config-driven
```

3. **En WSL2, instalar dependencias:**

```bash
sudo apt update && sudo apt install make dos2unix python3-pip -y
```

4. **Instalar dependencias Python:**

```bash
pip install -r requirements.txt
```

5. **Convertir scripts a formato Unix (solo una vez):**

```bash
dos2unix scripts/*.sh
```

### 🐧 Linux nativo

```bash
# Clonar repositorio
git clone https://github.com/mi-org/mvp-config-driven.git
cd mvp-config-driven

# Instalar dependencias del sistema
sudo apt update && sudo apt install docker.io docker-compose make python3-pip -y

# Instalar dependencias Python
pip install -r requirements.txt

# Configurar Docker (si es necesario)
sudo usermod -aG docker $USER
newgrp docker
```

### ☁️ Azure CLI y Terraform

```bash
# Instalar Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Instalar Terraform
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install terraform

# Verificar instalación
az --version
terraform --version
```

---

## 🚀 Ejecución del proyecto

### 🏠 Despliegue local

#### 1. **Configurar variables de entorno**

```bash
# Copiar archivo de ejemplo
cp .env.example .env

# Editar variables según tu entorno
nano .env
```

Variables principales:
```bash
# Entorno
ENVIRONMENT=local
LOG_LEVEL=INFO

# MinIO (local)
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio12345

# Opcional: Azure (para pruebas híbridas)
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
```

#### 2. **Levantar infraestructura local**

```bash
# Iniciar servicios (Spark + MinIO)
make up

# Verificar que los servicios estén corriendo
docker ps
```

#### 3. **Ejecutar un pipeline**

```bash
# Pipeline básico con configuración por defecto
make run

# Pipeline específico
make run DATASET=finanzas/payments_v1 ENV=local

# Con logs detallados
make run-debug
```

#### 4. **Ver resultados**

- **MinIO UI:** [http://localhost:9001](http://localhost:9001)
  - Usuario: `minio`
  - Password: `minio12345`
- **Spark UI:** [http://localhost:8080](http://localhost:8080)
- **Datasets procesados:** `s3a://silver/payments_v1/`

#### 5. **Apagar servicios**

```bash
make down
```

### ☁️ Despliegue en Azure

> 📖 **Documentación completa:** Ver [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)

#### 1. **Autenticación en Azure**

```bash
# Login en Azure
az login

# Configurar suscripción
az account set --subscription "your-subscription-id"

# Crear Service Principal (si no existe)
az ad sp create-for-rbac --name "mvp-data-pipeline-sp" \
  --role Contributor \
  --scopes /subscriptions/your-subscription-id
```

#### 2. **Configurar Terraform**

```bash
cd terraform

# Inicializar Terraform
terraform init

# Crear archivo de variables
cp terraform.tfvars.example terraform.tfvars

# Editar variables
nano terraform.tfvars
```

Variables principales en `terraform.tfvars`:
```hcl
# Configuración básica
resource_group_name = "rg-mvp-data-pipeline"
location           = "East US"
environment        = "dev"

# Azure AD Groups (crear previamente)
data_engineers_group_name = "DataEngineers"
data_scientists_group_name = "DataScientists"

# Configuración de seguridad
enable_rbac_audit = true
enable_azure_policies = true
enable_mfa_for_admin = true

# Base de datos
sql_database_backup_retention_days = 7
sql_database_geo_redundant_backup = true
```

#### 3. **Desplegar infraestructura**

```bash
# Planificar despliegue
terraform plan

# Aplicar cambios
terraform apply

# Obtener outputs importantes
terraform output
```

#### 4. **Configurar secretos en Key Vault**

```bash
# Obtener nombre del Key Vault
KV_NAME=$(terraform output -raw key_vault_name)

# Configurar secretos
az keyvault secret set --vault-name $KV_NAME --name "sql-connection-string" --value "your-connection-string"
az keyvault secret set --vault-name $KV_NAME --name "storage-account-key" --value "your-storage-key"
az keyvault secret set --vault-name $KV_NAME --name "event-hub-connection-string" --value "your-eventhub-connection"
```

#### 5. **Ejecutar pipeline en Azure**

```bash
# Configurar variables para Azure
export ENVIRONMENT=azure
export AZURE_KEY_VAULT_NAME=$KV_NAME
export AZURE_STORAGE_ACCOUNT=$(terraform output -raw storage_account_name)

# Ejecutar pipeline
python pipelines/spark_job.py --config config/datasets/finanzas/payments_v1/pipeline.yml --env azure
```

### 🔍 Monitoreo y observabilidad

#### Logs estructurados
```bash
# Ver logs en tiempo real
tail -f logs/pipeline.log

# Buscar por correlation ID
grep "correlation_id=abc123" logs/pipeline.log

# Ver métricas
grep "metrics" logs/pipeline.log | jq .
```

#### Azure Monitor (en Azure)
- **Application Insights:** Métricas y trazas
- **Log Analytics:** Consultas KQL
- **Alertas:** Configuradas automáticamente

---

## ⚙️ Configuración de pipelines

### 📋 Esquema de datos

En `config/datasets/.../schema.json` se define cada columna con nombre, tipo y si es requerida:

```json
{
  "type": "object",
  "properties": {
    "payment_id": { "type": "string" },
    "amount": { "type": "number" },
    "payment_date": { "type": ["string", "null"], "format": "date-time" },
    "updated_at": { "type": ["string", "null"], "format": "date-time" }
  },
  "required": ["payment_id", "amount"]
}
```

### 🔄 Estandarización

En `pipeline.yml`:

```yaml
standardization:
  timezone: America/Bogota
  rename:
    - { from: customerId, to: customer_id }
  casts:
    - { column: amount, to: "decimal(18,2)", on_error: null }
    - { column: payment_date, to: "timestamp", format_hint: "yyyy-MM-dd[ HH:mm:ss]" }
  defaults:
    - { column: currency, value: "CLP" }
  deduplicate:
    key: [payment_id]
    order_by: [updated_at desc]
```

### ✅ Reglas de calidad

En `expectations.yml` se definen validaciones:

```yaml
expectations:
  - { column: amount, rule: ">= 0", action: quarantine }
  - { column: payment_date, rule: "not null", action: reject }
```

---

## 🔒 Seguridad

> 📖 **Documentación completa:** Ver [docs/SECURITY.md](docs/SECURITY.md)

### 🛡️ Características de seguridad implementadas

- **🔐 Gestión de secretos:** Azure Key Vault con RBAC
- **🔒 Cifrado:** Datos en reposo y en tránsito (TLS 1.2+)
- **👥 Control de acceso:** Azure AD con grupos y roles personalizados
- **📊 Auditoría:** Logs estructurados con correlación
- **🚫 Protección:** WAF + NSG + Private Endpoints
- **🔑 Autenticación:** JWT tokens con expiración
- **🎭 Enmascaramiento:** Datos sensibles (PII)

### 🔧 Configuración de seguridad

```python
from src.utils.security import get_encryption_manager, require_permission

# Cifrado de datos sensibles
encryption_manager = get_encryption_manager()
encrypted_data = encryption_manager.encrypt_field("sensitive_value", "credit_card")

# Control de acceso con decoradores
@require_permission("data.read")
def read_sensitive_data():
    return "sensitive data"
```

---

## 🧪 Pruebas y CI/CD

### 🔬 Ejecutar pruebas

```bash
# Todas las pruebas
python -m pytest tests/ -v

# Pruebas específicas
python -m pytest tests/test_security.py -v
python -m pytest tests/test_azure_integration.py -v

# Con cobertura
python -m pytest tests/ --cov=src --cov-report=html
```

### 🔍 Validación de configuraciones

```bash
# Validar configuraciones YAML/JSON
./ci/check_config.sh

# Lint de código
python -m flake8 src/
python -m black src/ --check

# Validación de seguridad
python -m bandit -r src/
```

### 🚀 CI/CD Pipeline

- **`ci/lint.yml`:** Validación de código y configuraciones
- **`ci/test_dataset.yml`:** Pruebas con datasets mínimos
- **GitHub Actions:** Ejecución automática en PRs

---

## 📚 Documentación

### 📖 Guías disponibles

- **[SECURITY.md](docs/SECURITY.md):** Guía completa de seguridad
- **[DEPLOYMENT.md](docs/DEPLOYMENT.md):** Instrucciones de despliegue
- **[API Documentation](src/):** Documentación del código

### 🎯 Buenas prácticas

#### 🔧 Configuración
- Usar rutas S3 (`s3a://raw/...`) en lugar de rutas locales
- Mantener actualizado `schema.json` al cambiar columnas
- Agregar reglas de calidad en `expectations.yml`
- Versionar datasets (`payments_v1`, `payments_v2`)

#### 🔒 Seguridad
- Nunca hardcodear secretos en el código
- Usar Azure Key Vault para gestión de secretos
- Aplicar principio de menor privilegio
- Auditar todos los accesos a datos sensibles

#### 📊 Observabilidad
- Usar correlation IDs en todos los logs
- Implementar métricas de calidad de datos
- Configurar alertas para errores críticos
- Monitorear rendimiento de pipelines

---

## 🤝 Contribuciones

### 🔄 Proceso de desarrollo

1. **Crear rama:**
```bash
git checkout -b feature/nueva-funcionalidad
```

2. **Desarrollar:**
```bash
# Hacer cambios en código/configuración
# Agregar pruebas unitarias
# Actualizar documentación si es necesario
```

3. **Validar:**
```bash
# Ejecutar pruebas
python -m pytest tests/ -v

# Validar configuraciones
./ci/check_config.sh

# Probar localmente
make run
```

4. **Crear PR:**
```bash
git push origin feature/nueva-funcionalidad
# Crear Pull Request en GitHub
```

### 📋 Checklist para PRs

- [ ] ✅ Pruebas unitarias agregadas/actualizadas
- [ ] 🔒 Revisión de seguridad completada
- [ ] 📖 Documentación actualizada
- [ ] 🧪 Pruebas locales exitosas
- [ ] 🔍 Validación de configuraciones
- [ ] 📊 Logs estructurados implementados

---

## 📞 Soporte

### 🆘 Resolución de problemas

1. **Revisar logs:** `tail -f logs/pipeline.log`
2. **Verificar configuración:** `./ci/check_config.sh`
3. **Consultar documentación:** [docs/](docs/)
4. **Crear issue:** GitHub Issues

### 📧 Contacto

- **Equipo de Data Engineering:** data-engineering@company.com
- **Soporte técnico:** tech-support@company.com
- **Seguridad:** security@company.com
