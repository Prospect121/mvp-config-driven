terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.70.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.20.0"
    }
  }
}

provider "azurerm" {
  features {}
}

locals {
  databricks_host = var.databricks_host != ""
    ? var.databricks_host
    : (var.create_databricks_workspace
        ? azurerm_databricks_workspace.this[0].workspace_url
        : data.azurerm_databricks_workspace.existing[0].workspace_url)
}

provider "databricks" {
  host  = local.databricks_host
  token = var.databricks_token
}
