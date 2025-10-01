# Variables de configuración para el despliegue de Azure

variable "project_name" {
  description = "Nombre del proyecto"
  type        = string
  default     = "mvp-config-driven"
}

variable "resource_group_name" {
  description = "Nombre del grupo de recursos"
  type        = string
  default     = ""
}

variable "environment" {
  description = "Entorno de despliegue (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "location" {
  description = "Región de Azure"
  type        = string
  default     = "West US 2"
}

variable "owner" {
  description = "Propietario del proyecto"
  type        = string
  default     = "DataEngineering"
}

# Variables de seguridad
variable "enable_customer_managed_keys" {
  description = "Habilitar claves administradas por el cliente"
  type        = bool
  default     = true
}

variable "enable_private_endpoints" {
  description = "Habilitar endpoints privados"
  type        = bool
  default     = false
}

variable "allowed_ip_ranges" {
  description = "Rangos de IP permitidos para acceso"
  type        = list(string)
  default     = []
}

# Variables de red
variable "vnet_address_space" {
  description = "Espacio de direcciones de la VNet"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

variable "subnet_address_prefixes" {
  description = "Prefijos de direcciones de subnets"
  type = object({
    data_services = string
    private_endpoints = string
  })
  default = {
    data_services = "10.0.1.0/24"
    private_endpoints = "10.0.2.0/24"
  }
}

# Variables de base de datos
variable "sql_admin_username" {
  description = "Usuario administrador de SQL Server"
  type        = string
  default     = "sqladmin"
}

variable "sql_admin_password" {
  description = "Contraseña del administrador de SQL Server"
  type        = string
  sensitive   = true
  default     = null
}

variable "sql_database_sku" {
  description = "SKU de la base de datos SQL"
  type        = string
  default     = "S1"
}

variable "sql_server_name" {
  description = "Nombre del servidor SQL"
  type        = string
  default     = ""
}

variable "sql_database_name" {
  description = "Nombre de la base de datos SQL"
  type        = string
  default     = ""
}

variable "sql_sku_name" {
  description = "SKU de la base de datos SQL (alias)"
  type        = string
  default     = "S1"
}

# Variables de Event Hub
variable "eventhub_sku" {
  description = "SKU del Event Hub"
  type        = string
  default     = "Standard"
}

variable "eventhub_capacity" {
  description = "Capacidad del Event Hub"
  type        = number
  default     = 1
}

variable "eventhub_namespace_name" {
  description = "Nombre del namespace de Event Hub"
  type        = string
  default     = ""
}

# Variables de almacenamiento
variable "storage_account_tier" {
  description = "Tier de la cuenta de almacenamiento"
  type        = string
  default     = "Standard"
}

variable "storage_account_replication_type" {
  description = "Tipo de replicación del almacenamiento"
  type        = string
  default     = "LRS"
}

variable "storage_replication_type" {
  description = "Tipo de replicación del almacenamiento (alias)"
  type        = string
  default     = "LRS"
}

# Variables de Data Factory
variable "data_factory_managed_identity" {
  description = "Habilitar identidad administrada para Data Factory"
  type        = bool
  default     = true
}

variable "data_factory_name" {
  description = "Nombre del Data Factory"
  type        = string
  default     = ""
}

# Variables de Key Vault
variable "key_vault_sku" {
  description = "SKU del Key Vault"
  type        = string
  default     = "standard"
}

variable "key_vault_soft_delete_retention_days" {
  description = "Días de retención para soft delete en Key Vault"
  type        = number
  default     = 7
}

variable "key_vault_name" {
  description = "Nombre del Key Vault"
  type        = string
  default     = ""
}

# Configuración de monitoreo
variable "enable_monitoring" {
  description = "Habilitar servicios de monitoreo (Log Analytics, Application Insights)"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Días de retención para logs en Log Analytics"
  type        = number
  default     = 30
  validation {
    condition     = var.log_retention_days >= 30 && var.log_retention_days <= 730
    error_message = "Los días de retención deben estar entre 30 y 730."
  }
}

# Configuración de RBAC y seguridad
variable "data_engineers_group_name" {
  description = "Nombre del grupo de Azure AD para Data Engineers"
  type        = string
  default     = ""
}

variable "data_scientists_group_name" {
  description = "Nombre del grupo de Azure AD para Data Scientists"
  type        = string
  default     = ""
}

variable "enable_rbac_audit" {
  description = "Habilitar auditoría de RBAC en Key Vault"
  type        = bool
  default     = true
}

variable "enable_azure_policies" {
  description = "Habilitar Azure Policies para seguridad"
  type        = bool
  default     = true
}

variable "require_mfa_for_admin" {
  description = "Requerir MFA para operaciones administrativas"
  type        = bool
  default     = true
}

# Configuración de backup y disaster recovery
variable "enable_backup" {
  description = "Habilitar backup automático para SQL Database"
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "Días de retención para backups de SQL Database"
  type        = number
  default     = 7
  validation {
    condition     = var.backup_retention_days >= 7 && var.backup_retention_days <= 35
    error_message = "Los días de retención de backup deben estar entre 7 y 35."
  }
}

variable "geo_redundant_backup" {
  description = "Habilitar backup geo-redundante"
  type        = bool
  default     = false
}

# Variables de presupuesto
variable "monthly_limit" {
  description = "Límite mensual de gasto en USD"
  type        = number
  default     = 30
}

variable "budget_contact_email" {
  description = "Email de contacto para alertas de presupuesto"
  type        = string
  default     = ""
}

# Variables de tags
variable "common_tags" {
  description = "Tags comunes para todos los recursos"
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Tags para recursos específicos"
  type        = map(string)
  default     = {}
}