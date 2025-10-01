# Configuración de seguridad en tránsito y cifrado
# Este archivo define las políticas de seguridad para garantizar TLS 1.2+ y cifrado en tránsito

# Política para requerir TLS 1.2 mínimo en Storage Accounts - Comentado temporalmente
# resource "azurerm_resource_group_policy_assignment" "require_tls_12_storage" {
#   count                = var.enable_azure_policies ? 1 : 0
#   name                 = "require-tls-12-storage-${var.project_name}"
#   resource_group_id    = azurerm_resource_group.main.id
#   policy_definition_id = "/providers/Microsoft.Authorization/policyDefinitions/404c3081-a854-4457-ae30-26a93ef643f9"
#   display_name         = "Require minimum TLS version 1.2 for storage accounts"
#   description          = "Enforce minimum TLS version 1.2 for all storage accounts in the resource group"
#
#   parameters = jsonencode({
#     minimumTlsVersion = {
#       value = "TLS1_2"
#     }
#   })
#
#   metadata = jsonencode({
#     category = "Security"
#     version  = "1.0.0"
#   })
# }

# Política para requerir HTTPS en Storage Accounts - Comentado temporalmente
# resource "azurerm_resource_group_policy_assignment" "require_https_storage" {
#   count                = var.enable_azure_policies ? 1 : 0
#   name                 = "require-https-storage-${var.project_name}"
#   resource_group_id    = azurerm_resource_group.main.id
#   policy_definition_id = "/providers/Microsoft.Authorization/policyDefinitions/404c3081-a854-4457-ae30-26a93ef643f9"
#   display_name         = "Secure transfer to storage accounts should be enabled"
#   description          = "Require secure transfer (HTTPS) for all storage account operations"
#
#   metadata = jsonencode({
#     category = "Security"
#     version  = "1.0.0"
#   })
# }

# Política para requerir cifrado en tránsito para SQL Database - Comentado temporalmente
# resource "azurerm_resource_group_policy_assignment" "require_sql_encryption" {
#   count                = var.enable_azure_policies ? 1 : 0
#   name                 = "require-sql-encryption-${var.project_name}"
#   resource_group_id    = azurerm_resource_group.main.id
#   policy_definition_id = "/providers/Microsoft.Authorization/policyDefinitions/17k78e20-9358-41c9-923c-fb736d382a12"
#   display_name         = "Transparent Data Encryption on SQL databases should be enabled"
#   description          = "Require Transparent Data Encryption (TDE) for SQL databases"
#
#   metadata = jsonencode({
#     category = "Security"
#     version  = "1.0.0"
#   })
# }

# Configuración de TLS para Application Gateway (si se usa)
resource "azurerm_application_gateway" "main" {
  count               = var.enable_private_endpoints && var.enable_application_gateway ? 1 : 0
  name                = "agw-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  sku {
    name     = "WAF_v2"
    tier     = "WAF_v2"
    capacity = 2
  }

  gateway_ip_configuration {
    name      = "gateway-ip-config"
    subnet_id = azurerm_subnet.gateway[0].id
  }

  frontend_port {
    name = "https-port"
    port = 443
  }

  frontend_ip_configuration {
    name                 = "frontend-ip-config"
    public_ip_address_id = azurerm_public_ip.gateway[0].id
  }

  backend_address_pool {
    name = "backend-pool"
  }

  backend_http_settings {
    name                  = "backend-https-settings"
    cookie_based_affinity = "Disabled"
    port                  = 443
    protocol              = "Https"
    request_timeout       = 60
  }

  http_listener {
    name                           = "https-listener"
    frontend_ip_configuration_name = "frontend-ip-config"
    frontend_port_name             = "https-port"
    protocol                       = "Https"
    ssl_certificate_name           = "ssl-cert"
  }

  request_routing_rule {
    name                       = "routing-rule"
    rule_type                  = "Basic"
    http_listener_name         = "https-listener"
    backend_address_pool_name  = "backend-pool"
    backend_http_settings_name = "backend-https-settings"
    priority                   = 100
  }

  ssl_certificate {
    name     = "ssl-cert"
    data     = var.ssl_certificate_data
    password = var.ssl_certificate_password
  }

  # WAF Configuration
  waf_configuration {
    enabled          = true
    firewall_mode    = "Prevention"
    rule_set_type    = "OWASP"
    rule_set_version = "3.2"
  }

  tags = var.tags
}

# Public IP para Application Gateway
resource "azurerm_public_ip" "gateway" {
  count               = var.enable_private_endpoints && var.enable_application_gateway ? 1 : 0
  name                = "pip-agw-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  allocation_method   = "Static"
  sku                 = "Standard"

  tags = var.tags
}

# Subnet para Application Gateway - usando el subnet gateway definido en network.tf

# Configuración de Key Vault para certificados SSL
resource "azurerm_key_vault_certificate" "ssl_cert" {
  count        = var.enable_ssl_certificate ? 1 : 0
  name         = "ssl-certificate"
  key_vault_id = azurerm_key_vault.main.id

  certificate_policy {
    issuer_parameters {
      name = "Self"
    }

    key_properties {
      exportable = true
      key_size   = 2048
      key_type   = "RSA"
      reuse_key  = true
    }

    lifetime_action {
      action {
        action_type = "AutoRenew"
      }

      trigger {
        days_before_expiry = 30
      }
    }

    secret_properties {
      content_type = "application/x-pkcs12"
    }

    x509_certificate_properties {
      extended_key_usage = ["1.3.6.1.5.5.7.3.1"]

      key_usage = [
        "cRLSign",
        "dataEncipherment",
        "digitalSignature",
        "keyAgreement",
        "keyCertSign",
        "keyEncipherment",
      ]

      subject_alternative_names {
        dns_names = [
          "*.${var.project_name}.${var.environment}.local",
          "${var.project_name}.${var.environment}.local"
        ]
      }

      subject            = "CN=${var.project_name}.${var.environment}.local"
      validity_in_months = 12
    }
  }

  depends_on = [azurerm_key_vault_access_policy.terraform]
}

# Variables adicionales para seguridad
variable "enable_application_gateway" {
  description = "Habilitar Application Gateway con WAF"
  type        = bool
  default     = false
}

variable "enable_ssl_certificate" {
  description = "Habilitar certificado SSL autogenerado"
  type        = bool
  default     = false
}

variable "ssl_certificate_data" {
  description = "Datos del certificado SSL (base64)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "ssl_certificate_password" {
  description = "Contraseña del certificado SSL"
  type        = string
  default     = ""
  sensitive   = true
}

# Configuración de Network Security para forzar HTTPS
resource "azurerm_network_security_rule" "deny_http" {
  count                       = var.enable_private_endpoints ? 1 : 0
  name                        = "DenyHTTP"
  priority                    = 1000
  direction                   = "Inbound"
  access                      = "Deny"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "80"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.main.name
  network_security_group_name = azurerm_network_security_group.data_services[0].name
}

resource "azurerm_network_security_rule" "allow_https_only" {
  count                       = var.enable_private_endpoints ? 1 : 0
  name                        = "AllowHTTPS"
  priority                    = 1001
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "443"
  source_address_prefix       = "VirtualNetwork"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.main.name
  network_security_group_name = azurerm_network_security_group.data_services[0].name
}