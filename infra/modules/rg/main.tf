resource "azurerm_resource_group" "this" {
  name     = var.name
  location = var.region
}
output "name" { value = azurerm_resource_group.this.name }
variable "name" {}
variable "region" {}
