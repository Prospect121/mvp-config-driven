variable "rg_name" {}
variable "region" {}
variable "account_name" {}
variable "containers" { type = list(string) }
variable "hns_enabled" { type = bool }

resource "azurerm_storage_account" "this" {
  name                     = var.account_name
  resource_group_name      = var.rg_name
  location                 = var.region
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = var.hns_enabled
  allow_nested_items_to_be_public = false
}

resource "azurerm_storage_container" "cts" {
  for_each              = toset(var.containers)
  name                  = each.value
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

output "account_name" { value = azurerm_storage_account.this.name }
output "containers"   { value = [for c in azurerm_storage_container.cts : c.name] }
