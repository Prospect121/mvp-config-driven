#!/usr/bin/env python3
"""
Script para preparar el despliegue en Azure del MVP Config-Driven Data Pipeline
"""

import os
import sys
import json
import yaml
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

def check_azure_cli():
    """Verifica que Azure CLI esté instalado y configurado"""
    print("🔍 Verificando Azure CLI...")
    
    # Intentar diferentes formas de ejecutar az en Windows
    az_commands = ['az', 'az.cmd', 'az.exe']
    
    for az_cmd in az_commands:
        try:
            result = subprocess.run([az_cmd, '--version'], 
                                  capture_output=True, text=True, check=True, shell=True)
            print("✅ Azure CLI está instalado")
            
            # Verificar login
            try:
                result = subprocess.run([az_cmd, 'account', 'show'], 
                                      capture_output=True, text=True, check=True, shell=True)
                account_info = json.loads(result.stdout)
                print(f"✅ Conectado a Azure como: {account_info.get('user', {}).get('name', 'N/A')}")
                print(f"✅ Suscripción activa: {account_info.get('name', 'N/A')}")
                return True
            except subprocess.CalledProcessError as e:
                if "Please run 'az login'" in e.stderr or "az login" in e.stderr:
                    print("⚠️  Azure CLI instalado pero no está logueado")
                    print("💡 Ejecutar: az login")
                    return False
                else:
                    print("⚠️  Azure CLI instalado pero hay un problema de configuración")
                    print(f"Error: {e.stderr}")
                    return False
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    
    print("❌ Azure CLI no está instalado o no se puede ejecutar")
    print("💡 Instalar: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli")
    print("💡 Login: az login")
    return False

def check_terraform():
    """Verifica que Terraform esté instalado"""
    print("🔍 Verificando Terraform...")
    
    try:
        result = subprocess.run(['terraform', '--version'], 
                              capture_output=True, text=True, check=True)
        print("✅ Terraform está instalado")
        return True
        
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Terraform no está instalado")
        print("💡 Instalar: https://www.terraform.io/downloads.html")
        return False

def load_azure_config() -> Dict:
    """Carga la configuración de Azure"""
    config_file = Path("config/azure_deployment.yml")
    
    if not config_file.exists():
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_file}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def validate_azure_config(config: Dict) -> List[str]:
    """Valida la configuración de Azure"""
    print("🔍 Validando configuración de Azure...")
    
    errors = []
    
    # Validar secciones requeridas
    required_sections = ['deployment', 'resources', 'security', 'monitoring']
    for section in required_sections:
        if section not in config:
            errors.append(f"Sección requerida faltante: {section}")
    
    # Validar configuración de deployment
    if 'deployment' in config:
        deployment = config['deployment']
        required_fields = ['name', 'environment', 'region']
        for field in required_fields:
            if field not in deployment:
                errors.append(f"Campo requerido faltante en deployment: {field}")
    
    # Validar recursos
    if 'resources' in config:
        resources = config['resources']
        required_resources = ['resource_group', 'storage', 'sql_database']
        for resource in required_resources:
            if resource not in resources:
                errors.append(f"Recurso requerido faltante: {resource}")
    
    if errors:
        print("❌ Errores de validación encontrados:")
        for error in errors:
            print(f"  • {error}")
    else:
        print("✅ Configuración de Azure válida")
    
    return errors

def create_terraform_vars(config: Dict) -> str:
    """Crea el archivo terraform.tfvars basado en la configuración"""
    print("📝 Generando terraform.tfvars...")
    
    deployment = config['deployment']
    resources = config['resources']
    security = config['security']
    
    tfvars_content = f"""# =============================================================================
# Terraform Variables - Generado automáticamente
# Generado el: {datetime.now().isoformat()}
# =============================================================================

# Configuración básica
resource_group_name = "{resources['resource_group']['name']}"
location           = "{resources['resource_group']['location']}"
environment        = "{deployment['environment']}"
project_name       = "{deployment['name']}"

# Configuración de seguridad
enable_rbac_audit     = {str(security['rbac']['enable_audit']).lower()}
enable_azure_policies = {str(security['rbac']['enable_policies']).lower()}

# Configuración de almacenamiento
storage_account_tier             = "{resources['storage']['tier']}"
storage_account_replication_type = "{resources['storage']['replication']}"

# Configuración de SQL Database
sql_server_name   = "{resources['sql_database']['server_name']}"
sql_database_name = "{resources['sql_database']['database_name']}"
sql_sku_name      = "{resources['sql_database']['sku']}"

# Configuración de Event Hub
eventhub_namespace_name = "{resources['event_hub']['namespace']}"
eventhub_sku           = "{resources['event_hub']['sku']}"

# Configuración de Key Vault
key_vault_name = "{resources['key_vault']['name']}"
key_vault_sku  = "{resources['key_vault']['sku']}"

# Configuración de Data Factory
data_factory_name = "{resources['data_factory']['name']}"

# Configuración de monitoreo
enable_monitoring = {str(config['monitoring']['enable_monitoring']).lower()}

# Rangos de IP permitidos
allowed_ip_ranges = {json.dumps(security['network']['allowed_ip_ranges'])}

# Grupos de Azure AD
data_engineers_group_name  = "{security['azure_ad_groups']['data_engineers']}"
data_scientists_group_name = "{security['azure_ad_groups']['data_scientists']}"

# Tags comunes
common_tags = {{
  Environment = "{deployment['environment']}"
  Project     = "{deployment['name']}"
  ManagedBy   = "Terraform"
  CreatedDate = "{datetime.now().strftime('%Y-%m-%d')}"
}}
"""
    
    tfvars_file = Path("terraform/terraform.tfvars")
    with open(tfvars_file, 'w', encoding='utf-8') as f:
        f.write(tfvars_content)
    
    print(f"✅ Archivo terraform.tfvars creado: {tfvars_file}")
    return str(tfvars_file)

def create_environment_file(config: Dict) -> str:
    """Crea el archivo .env para Azure"""
    print("📝 Generando archivo .env.azure...")
    
    env_vars = config.get('environment_variables', {})
    
    env_content = f"""# =============================================================================
# Variables de Entorno para Azure - Generado automáticamente
# Generado el: {datetime.now().isoformat()}
# =============================================================================

# Configuración de Azure
AZURE_SUBSCRIPTION_ID={os.getenv('AZURE_SUBSCRIPTION_ID', 'your-subscription-id')}
AZURE_TENANT_ID={os.getenv('AZURE_TENANT_ID', 'your-tenant-id')}
AZURE_CLIENT_ID={os.getenv('AZURE_CLIENT_ID', 'your-client-id')}
AZURE_CLIENT_SECRET={os.getenv('AZURE_CLIENT_SECRET', 'your-client-secret')}

# Configuración de recursos (se actualizarán después del despliegue)
AZURE_STORAGE_ACCOUNT={env_vars.get('AZURE_STORAGE_ACCOUNT', 'storage-account-name')}
AZURE_STORAGE_KEY=your-storage-key
AZURE_SQL_SERVER={env_vars.get('AZURE_SQL_SERVER', 'sql-server-name')}
AZURE_SQL_DATABASE={env_vars.get('AZURE_SQL_DATABASE', 'database-name')}
AZURE_SQL_USERNAME=your-sql-username
AZURE_SQL_PASSWORD=your-sql-password
AZURE_EVENTHUB_NAMESPACE={env_vars.get('AZURE_EVENTHUB_NAMESPACE', 'eventhub-namespace')}
AZURE_EVENTHUB_CONNECTION_STRING=your-eventhub-connection-string
AZURE_APPINSIGHTS_INSTRUMENTATION_KEY=your-appinsights-key

# Configuración general
ENVIRONMENT={env_vars.get('ENVIRONMENT', 'dev')}
LOG_LEVEL={env_vars.get('LOG_LEVEL', 'INFO')}
ENABLE_MONITORING={env_vars.get('ENABLE_MONITORING', 'true')}

# Configuración de pipeline
SPARK_CLUSTER_NAME={config.get('pipeline', {}).get('spark', {}).get('cluster_name', 'spark-cluster')}
BATCH_SIZE={config.get('pipeline', {}).get('processing', {}).get('batch_size', 1000)}
MAX_RETRIES={config.get('pipeline', {}).get('processing', {}).get('max_retries', 3)}
"""
    
    env_file = Path(".env.azure")
    with open(env_file, 'w', encoding='utf-8') as f:
        f.write(env_content)
    
    print(f"✅ Archivo .env.azure creado: {env_file}")
    return str(env_file)

def create_deployment_checklist(config: Dict) -> str:
    """Crea una checklist de despliegue"""
    print("📋 Generando checklist de despliegue...")
    
    checklist_content = f"""# Checklist de Despliegue en Azure
## MVP Config-Driven Data Pipeline

Generado el: {datetime.now().isoformat()}

### ✅ Pre-requisitos
- [ ] Azure CLI instalado y configurado
- [ ] Terraform instalado
- [ ] Permisos de Contributor en la suscripción de Azure
- [ ] Grupos de Azure AD creados:
  - [ ] {config['security']['azure_ad_groups']['data_engineers']}
  - [ ] {config['security']['azure_ad_groups']['data_scientists']}
  - [ ] {config['security']['azure_ad_groups']['administrators']}

### ✅ Configuración
- [ ] Archivo terraform.tfvars configurado
- [ ] Variables de entorno de Azure configuradas
- [ ] Configuración de backend de Terraform (opcional)

### ✅ Despliegue de Infraestructura
```bash
# 1. Inicializar Terraform
cd terraform
terraform init

# 2. Planificar despliegue
terraform plan -var-file="terraform.tfvars"

# 3. Aplicar cambios
terraform apply -var-file="terraform.tfvars"
```

### ✅ Post-Despliegue
- [ ] Verificar recursos creados en Azure Portal
- [ ] Configurar secretos en Key Vault
- [ ] Probar conectividad a recursos
- [ ] Configurar alertas y monitoreo
- [ ] Ejecutar pipeline de prueba

### ✅ Recursos Creados
- [ ] Resource Group: {config['resources']['resource_group']['name']}
- [ ] Storage Account: {config['resources']['storage']['account_name']}
- [ ] SQL Server: {config['resources']['sql_database']['server_name']}
- [ ] Event Hub: {config['resources']['event_hub']['namespace']}
- [ ] Key Vault: {config['resources']['key_vault']['name']}
- [ ] Data Factory: {config['resources']['data_factory']['name']}
- [ ] Application Insights: {config['resources']['app_insights']['name']}

### ✅ Validación
- [ ] Pipeline ejecuta correctamente
- [ ] Datos se procesan y almacenan
- [ ] Monitoreo funciona
- [ ] Alertas configuradas
- [ ] Seguridad implementada

### 🚨 Troubleshooting
Si encuentras problemas:
1. Verificar logs en Application Insights
2. Revisar configuración de red y firewall
3. Validar permisos de RBAC
4. Consultar documentación en docs/DEPLOYMENT.md

### 📞 Contacto
Para soporte técnico, contactar al equipo de Data Engineering.
"""
    
    checklist_file = Path("AZURE_DEPLOYMENT_CHECKLIST.md")
    with open(checklist_file, 'w', encoding='utf-8') as f:
        f.write(checklist_content)
    
    print(f"✅ Checklist de despliegue creado: {checklist_file}")
    return str(checklist_file)

def generate_deployment_summary(config: Dict) -> Dict:
    """Genera un resumen del despliegue"""
    return {
        "deployment_info": {
            "name": config['deployment']['name'],
            "environment": config['deployment']['environment'],
            "region": config['deployment']['region'],
            "generated_at": datetime.now().isoformat()
        },
        "resources_to_create": {
            "resource_group": config['resources']['resource_group']['name'],
            "storage_account": config['resources']['storage']['account_name'],
            "sql_server": config['resources']['sql_database']['server_name'],
            "event_hub": config['resources']['event_hub']['namespace'],
            "key_vault": config['resources']['key_vault']['name'],
            "data_factory": config['resources']['data_factory']['name']
        },
        "estimated_monthly_cost": {
            "budget_limit": config.get('cost_management', {}).get('budget', {}).get('monthly_limit', 30),
            "currency": "USD",
            "note": "Estimación basada en configuración dev"
        },
        "next_steps": [
            "Revisar terraform.tfvars generado",
            "Configurar variables de entorno de Azure",
            "Ejecutar terraform plan para revisar cambios",
            "Ejecutar terraform apply para crear recursos",
            "Seguir checklist de post-despliegue"
        ]
    }

def main():
    """Función principal"""
    print("🚀 Preparando despliegue en Azure...")
    print("=" * 60)
    
    # Verificar herramientas
    tools_ok = True
    if not check_azure_cli():
        tools_ok = False
    if not check_terraform():
        tools_ok = False
    
    if not tools_ok:
        print("\n❌ Herramientas requeridas faltantes. Instalar antes de continuar.")
        return 1
    
    try:
        # Cargar y validar configuración
        config = load_azure_config()
        validation_errors = validate_azure_config(config)
        
        if validation_errors:
            print("\n❌ Configuración inválida. Corregir errores antes de continuar.")
            return 1
        
        # Generar archivos de configuración
        tfvars_file = create_terraform_vars(config)
        env_file = create_environment_file(config)
        checklist_file = create_deployment_checklist(config)
        
        # Generar resumen
        summary = generate_deployment_summary(config)
        summary_file = Path("azure_deployment_summary.json")
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        print("\n" + "=" * 60)
        print("🎉 ¡Preparación completada exitosamente!")
        print("\n📁 Archivos generados:")
        print(f"  • {tfvars_file}")
        print(f"  • {env_file}")
        print(f"  • {checklist_file}")
        print(f"  • {summary_file}")
        
        print("\n🔄 Próximos pasos:")
        for step in summary['next_steps']:
            print(f"  1. {step}")
        
        print(f"\n💰 Presupuesto estimado: ${summary['estimated_monthly_cost']['budget_limit']} USD/mes")
        print("\n📖 Consultar AZURE_DEPLOYMENT_CHECKLIST.md para instrucciones detalladas")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error durante la preparación: {e}")
        return 1

if __name__ == "__main__":
    exit(main())