# Guía de Instalación

Esta guía cubre la instalación de `datacore` en entornos locales, de desarrollo y producción en múltiples nubes.

## Requisitos previos

- **Python**: 3.10 o superior
- **Java**: JDK 8 u 11 (requerido por PySpark)
- **Git**: Para clonar el repositorio

## Instalación local

### 1. Clonar el repositorio

```bash
git clone <repository-url>
cd mvp-config-driven
```

### 2. Crear entorno virtual

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux/macOS
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Actualizar pip e instalar datacore

```bash
pip install --upgrade pip
pip install -e .
```

### 4. Instalar dependencias opcionales

#### Para desarrollo

```bash
pip install -e ".[dev]"
```

Incluye: `pytest`, `pytest-cov`, `ruff`, `black`

#### Para Microsoft Fabric

```bash
pip install -e ".[fabric]"
```

Incluye: `azure-identity`, `azure-keyvault-secrets`, `delta-spark`, `azure-kusto-data`, `azure-kusto-ingest`, `pyodbc`

### 5. Verificar instalación

```bash
prodi --help
```

Deberías ver los comandos disponibles: `validate`, `run`, `plan`.

## Validar configuración

```bash
prodi validate --config configs/envs/dev/project.yml
```

Si la validación es exitosa, verás:

```
Configuración válida: configs/envs/dev/project.yml
```

## Ejecutar un pipeline local

```bash
# Ejecutar capa bronze en modo local
prodi run --layer bronze --config configs/envs/dev/layers/bronze.yml --platform local
```

Para ejecutar en modo dry-run (solo planificación):

```bash
prodi run --layer bronze --config configs/envs/dev/layers/bronze.yml --dry-run
```

## Configuración de entorno

### Variables de entorno

Crea un archivo `.env` en la raíz del proyecto:

```bash
# Configuración general
DATAENV=dev
LAYER=bronze

# Azure
AZURE_STORAGE_ACCOUNT=mystorageaccount
AZURE_STORAGE_KEY=${SECRET:AZURE_KEY}

# AWS
AWS_ACCESS_KEY_ID=${SECRET:AWS_ACCESS_KEY}
AWS_SECRET_ACCESS_KEY=${SECRET:AWS_SECRET_KEY}
AWS_REGION=us-east-1

# GCP
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# JDBC
ERP_JDBC_URL=jdbc:postgresql://localhost:5432/erp
CRM_JDBC_URL=jdbc:postgresql://localhost:5432/crm
```

### Secrets

Para resolución de secretos en la nube:

- **Azure**: Usa Azure Key Vault
- **AWS**: Usa AWS Secrets Manager
- **GCP**: Usa Google Secret Manager
- **Fabric**: Usa Azure Key Vault o variables de entorno

Referencia secretos en YAML con `${SECRET:ALIAS}`:

```yaml
source:
  type: jdbc
  url: ${SECRET:DB_CONNECTION_STRING}
  options:
    user: ${SECRET:DB_USER}
    password: ${SECRET:DB_PASSWORD}
```

## Despliegue en la nube

### Azure Databricks

Consulta [docs/deploy/azure_databricks.md](deploy/azure_databricks.md) para:
- Instalación del paquete en cluster
- Configuración de notebooks
- Secrets Scope
- Ejecución de jobs

### AWS Glue

Consulta [docs/deploy/aws_glue.md](deploy/aws_glue.md) para:
- Empaquetado para Glue
- Configuración de Glue Job
- Parámetros de ejecución
- Roles y permisos IAM

### GCP Dataproc

Consulta [docs/deploy/gcp_dataproc.md](deploy/gcp_dataproc.md) para:
- Creación de cluster
- Instalación de dependencias
- Ejecución de jobs
- Service Account setup

### Microsoft Fabric

Para Fabric Lakehouse y Spark notebooks:

1. Instala dependencias adicionales:
```bash
pip install -e ".[fabric]"
```

2. Configura el workspace en `configs/platforms/fabric.yml`:
```yaml
platform: fabric
providers:
  fabric:
    workspace_id: ${FABRIC_WORKSPACE_ID}
    lakehouse_id: ${FABRIC_LAKEHOUSE_ID}
```

3. Lee la documentación específica: [docs/fabric.md](fabric.md)

## Solución de problemas

### Error: "SparkSession not available"

Si ejecutas en local sin Spark instalado, asegúrate de que:
1. Java esté instalado (verifica con `java -version`)
2. PySpark esté instalado (`pip list | grep pyspark`)

### Error: "Module not found: datacore"

Reinstala el paquete en modo editable:
```bash
pip install -e .
```

### Error de permisos en cloud storage

Verifica que:
- Las credenciales estén configuradas correctamente
- Los roles/permisos incluyan acceso de lectura/escritura
- Las URIs usen el esquema correcto (`s3://`, `abfs://`, `gs://`)

## Próximos pasos

- Lee [docs/configuration.md](configuration.md) para entender la estructura de configuración
- Revisa [docs/use_cases.md](use_cases.md) para ejemplos prácticos
- Consulta [docs/architecture.md](architecture.md) para comprender el diseño del sistema
