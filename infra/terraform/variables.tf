variable "prefix" {
  description = "Prefijo de nombres para recursos"
  type        = string
  default     = "mvp-cfg"
}

variable "location" {
  description = "Región de Azure"
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Nombre del Resource Group"
  type        = string
  default     = null
}

variable "storage_account_name" {
  description = "Nombre del Storage Account (ADLS Gen2)"
  type        = string
  default     = null
}

variable "dataset_config_path" {
  description = "Ruta relativa del dataset.yml dentro del filesystem (por ejemplo: datasets/finanzas/payments_v1/dataset.yml)"
  type        = string
  default     = "datasets/finanzas/payments_v1/dataset.yml"
}

variable "environment_name" {
  description = "Nombre del entorno para el pipeline"
  type        = string
  default     = "default"
}

variable "spark_version" {
  description = "Versión de Databricks Runtime"
  type        = string
  default     = "14.3.x-scala2.12"
}

variable "node_type_id" {
  description = "Tipo de nodo de cluster"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "num_workers" {
  description = "Número de workers para el job cluster"
  type        = number
  default     = 2
}

variable "aad_client_id" {
  description = "Client ID para OAuth de ABFS (opcional si usas identidad administrada)"
  type        = string
  default     = null
  sensitive   = true
}

variable "aad_client_secret" {
  description = "Client Secret para OAuth de ABFS"
  type        = string
  default     = null
  sensitive   = true
}

variable "tenant_id" {
  description = "Tenant ID de Azure AD"
  type        = string
  default     = null
}

variable "subscription_id" {
  description = "ID de suscripción de Azure"
  type        = string
  default     = null
}

variable "package_name" {
  description = "Nombre del paquete Python (wheel)"
  type        = string
  default     = "mvp_config_driven"
}

variable "package_wheel_filename" {
  description = "Nombre del archivo .whl en dist/"
  type        = string
  default     = "mvp_config_driven-0.1.0-py3-none-any.whl"
}

variable "local_wheel_path" {
  description = "Ruta local al wheel a subir a DBFS; si se omite, se deriva desde dist/"
  type        = string
  default     = null
}

variable "local_config_root" {
  description = "Ruta local al directorio de configuración"
  type        = string
  default     = null
}