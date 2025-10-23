terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.100.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.35.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
}

# Configura el provider de Databricks apuntando al workspace creado por azurerm
provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.dbw.id
}