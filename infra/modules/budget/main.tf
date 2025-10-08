variable "subscription_id" {}
variable "name" {}
variable "amount" { type = number }

resource "azurerm_consumption_budget_subscription" "this" {
  name            = var.name
  amount          = var.amount
  time_grain      = "Monthly"
  subscription_id = var.subscription_id

  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
    end_date   = formatdate("YYYY-12-31'T'23:59:59Z", timestamp()) # arbitrario
  }

  notification {
    enabled        = true
    threshold      = 50
    operator       = "GreaterThan"
    contact_emails = [var.contact_email != "" ? var.contact_email : "you@example.com"]
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    contact_emails = [var.contact_email != "" ? var.contact_email : "you@example.com"]
  }

  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThanOrEqualTo"
    contact_emails = [var.contact_email != "" ? var.contact_email : "you@example.com"]
  }
}

variable "contact_email" {
  type    = string
  default = ""
}


