terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.100.0, < 5.0.0"
    }
    # Deja databricks solo si tienes recursos .tf de databricks en esta carpeta
    # databricks = {
    #   source  = "databricks/databricks"
    #   version = ">= 1.50.0"
    # }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.0"
    }
  }
}

provider "azurerm" {
  features {}
}

locals {
  name          = var.name
  region        = var.region
}

data "azurerm_subscription" "sub" {}

resource "random_string" "suffix" {
  length  = 4
  upper   = false
  special = false
}

# Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.name}-dev"
  location = local.region
}

# Storage Account + contenedores
resource "azurerm_storage_account" "st" {
  name                            = "st${local.name}dev${random_string.suffix.result}"
  resource_group_name             = azurerm_resource_group.rg.name
  location                        = local.region
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true
  allow_nested_items_to_be_public = false
}

resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_id    = azurerm_storage_account.st.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_id    = azurerm_storage_account.st.id
  container_access_type = "private"
}

# ACR
resource "azurerm_container_registry" "acr" {
  name                = "acr${local.name}dev${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = local.region
  sku                 = "Basic"
  admin_enabled       = false
}

# Identidad administrada para ACI
resource "azurerm_user_assigned_identity" "uami" {
  name                = "uami-${local.name}-runner"
  location            = local.region
  resource_group_name = azurerm_resource_group.rg.name
}

# Permiso AcrPull para la UAMI
resource "azurerm_role_assignment" "acr_pull" {
  scope                = azurerm_container_registry.acr.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.uami.principal_id
}

# Presupuesto de suscripción
resource "azurerm_consumption_budget_subscription" "budget" {
  name            = "budget-${local.name}-dev"
  subscription_id = data.azurerm_subscription.sub.id
  amount          = var.budget_amount
  time_grain      = "Monthly"

  time_period {
    start_date = formatdate("YYYY-MM-01", timestamp())
    end_date   = formatdate("YYYY-12-31", timestamp())
  }

  notification {
    enabled        = true
    operator       = "GreaterThan"
    threshold      = 80
    contact_emails = ["ericknieto44@gmail.com"]
  }

  notification {
    enabled        = true
    operator       = "GreaterThanOrEqualTo"
    threshold      = 100
    contact_emails = ["ericknieto44@gmail.com"]
  }
}

# ACI runner (smoke-test con imagen pública)
resource "azurerm_container_group" "aci" {
  name                = "aci-${local.name}-runner"
  location            = local.region
  resource_group_name = azurerm_resource_group.rg.name
  os_type             = "Linux"
  ip_address_type     = "None"
  restart_policy      = "Always"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.uami.id]
  }

  container {
    name   = "runner"
    image  = var.runner_image != "" ? var.runner_image : "bitnami/spark:3.5.1"
    cpu    = 2
    memory = 4

    environment_variables = {
      STORAGE_ACCOUNT = azurerm_storage_account.st.name
      STORAGE_KEY     = azurerm_storage_account.st.primary_access_key
      TZ              = "America/Bogota"
    }

    commands = [
      "/bin/sh",
      "-lc",
      "if [ -n \"$STORAGE_KEY\" ]; then sed -i \"s/<STORAGE>/${STORAGE_ACCOUNT}/g\" /opt/bitnami/spark/conf/spark-defaults.conf && echo \"spark.hadoop.fs.azure.account.key.${STORAGE_ACCOUNT}.dfs.core.windows.net  ${STORAGE_KEY}\" >> /opt/bitnami/spark/conf/spark-defaults.conf; fi; sleep infinity"
    ]
  }
}
