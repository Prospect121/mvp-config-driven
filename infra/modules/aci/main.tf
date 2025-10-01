variable "rg_name" {}
variable "region" {}
variable "name" {}
variable "cpu" {}
variable "memory_gb" {}
variable "image" {}
variable "env_vars" {
  type    = map(string)
  default = {}
}

resource "azurerm_container_group" "this" {
  name                = var.name
  resource_group_name = var.rg_name
  location            = var.region
  os_type             = "Linux"
  ip_address_type     = "None"

  container {
    name   = "runner"
    image  = var.image
    cpu    = var.cpu
    memory = var.memory_gb

    environment_variables = var.env_vars

    commands = [
      "/bin/sh","-lc",
      # aquí podrías `curl` el zip del repo o usar az storage blob download y luego spark-submit
      "echo ACI listo; sleep 3600"
    ]
  }
}
