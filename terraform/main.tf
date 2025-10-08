# Configuración principal de Terraform para Azure Data Platform
terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.0"
    }
  }
}

# Configuración del proveedor Azure
provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  
  # Usar autenticación basada en identidad para storage accounts
  storage_use_azuread = true
}

# Datos del cliente actual
data "azurerm_client_config" "current" {}

# Grupo de recursos principal
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location

  tags = local.common_tags
}

# Variables locales
locals {
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
    Owner       = var.owner
  }
  
  # Nombres de recursos - Estándares Corporativos Prodigio
  resource_prefix     = "${var.project_name}-${var.environment}"
  storage_account_name = "pdg${var.environment}sa001"
  key_vault_name      = "pdg-${var.environment}-kv-001"
  sql_server_name     = "${var.project_name}-${var.environment}-sql-001"
  eventhub_namespace  = "${var.project_name}-${var.environment}-eh-001"
  data_factory_name   = "${var.project_name}-${var.environment}-df-001"
  allowed_ip_ranges   = var.allowed_ip_ranges
}

# Configuración de presupuesto
module "budget" {
  source          = "../infra/modules/budget"
  subscription_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"
  name           = "${var.project_name}-${var.environment}-budget-001"
  amount         = var.monthly_limit
  contact_email  = var.budget_contact_email
}