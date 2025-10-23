# Resource Group
resource "azurerm_resource_group" "rg" {
  name     = coalesce(var.resource_group_name, "${var.prefix}-rg")
  location = var.location
}

# Storage Account (ADLS Gen2)
resource "azurerm_storage_account" "adls" {
  name                     = coalesce(var.storage_account_name, replace(lower("${var.prefix}adls"), "-", ""))
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
  allow_nested_items_to_be_public = false
}

# Filesystem para configuraciones
resource "azurerm_storage_data_lake_gen2_filesystem" "configs" {
  name               = "configs"
  storage_account_id = azurerm_storage_account.adls.id
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "dbw" {
  name                = "${var.prefix}-dbw"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
}