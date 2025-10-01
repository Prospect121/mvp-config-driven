variable "rg_name" {}
variable "region" {}
variable "name" {}
variable "sku" {}

resource "azurerm_key_vault" "this" {
  name                        = var.name
  location                    = var.region
  resource_group_name         = var.rg_name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = var.sku
  purge_protection_enabled    = false
  soft_delete_retention_days  = 7
}

data "azurerm_client_config" "current" {}
output "name" { value = azurerm_key_vault.this.name }
