variable "name" {
  type    = string
  default = "mvp"
}

variable "region" {
  type    = string
  default = "eastus2"
}

variable "budget_amount" {
  type    = number
  default = 10
}

variable "runner_image" {
  # Imagen del runner en ACR, ej: acrmvpdevabcd.azurecr.io/runner:latest
  type    = string
  default = ""
}
