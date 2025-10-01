# Configuración de RBAC y políticas de seguridad para Azure Key Vault

# Obtener información del grupo de Azure AD para Data Engineers
data "azuread_group" "data_engineers" {
  count        = var.data_engineers_group_name != "" ? 1 : 0
  display_name = var.data_engineers_group_name
}

# Obtener información del grupo de Azure AD para Data Scientists
data "azuread_group" "data_scientists" {
  count        = var.data_scientists_group_name != "" ? 1 : 0
  display_name = var.data_scientists_group_name
}

# Role Assignment: Data Factory Managed Identity - Key Vault Secrets User
resource "azurerm_role_assignment" "data_factory_keyvault_secrets_user" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# Role Assignment: Data Factory Managed Identity - Storage Blob Data Contributor
# Comentado porque ya existe una asignación similar en datafactory.tf
# resource "azurerm_role_assignment" "data_factory_storage_contributor" {
#   scope                = azurerm_storage_account.adls.id
#   role_definition_name = "Storage Blob Data Contributor"
#   principal_id         = azurerm_data_factory.main.identity[0].principal_id
# }

# Role Assignment: Data Factory Managed Identity - SQL DB Contributor
resource "azurerm_role_assignment" "data_factory_sql_contributor" {
  scope                = azurerm_mssql_database.main.id
  role_definition_name = "SQL DB Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
  
  # Evitar conflictos con asignaciones existentes
  lifecycle {
    create_before_destroy = true
  }
}

# Role Assignment: Data Factory Managed Identity - Event Hubs Data Owner
resource "azurerm_role_assignment" "data_factory_eventhub_owner" {
  scope                = azurerm_eventhub_namespace.main.id
  role_definition_name = "Azure Event Hubs Data Owner"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# Role Assignment: Data Engineers Group - Key Vault Administrator
resource "azurerm_role_assignment" "data_engineers_keyvault_admin" {
  count                = length(data.azuread_group.data_engineers) > 0 ? 1 : 0
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azuread_group.data_engineers[0].object_id
}

# Role Assignment: Data Engineers Group - Storage Blob Data Contributor
resource "azurerm_role_assignment" "data_engineers_storage_contributor" {
  count                = length(data.azuread_group.data_engineers) > 0 ? 1 : 0
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_group.data_engineers[0].object_id
}

# Role Assignment: Data Engineers Group - SQL DB Contributor
resource "azurerm_role_assignment" "data_engineers_sql_contributor" {
  count                = length(data.azuread_group.data_engineers) > 0 ? 1 : 0
  scope                = azurerm_mssql_database.main.id
  role_definition_name = "SQL DB Contributor"
  principal_id         = data.azuread_group.data_engineers[0].object_id
  
  lifecycle {
    create_before_destroy = true
  }
}

# Role Assignment: Data Scientists Group - Key Vault Secrets User (read-only)
resource "azurerm_role_assignment" "data_scientists_keyvault_secrets_user" {
  count                = length(data.azuread_group.data_scientists) > 0 ? 1 : 0
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = data.azuread_group.data_scientists[0].object_id
}

# Role Assignment: Data Scientists Group - Storage Blob Data Reader
resource "azurerm_role_assignment" "data_scientists_storage_reader" {
  count                = length(data.azuread_group.data_scientists) > 0 ? 1 : 0
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = data.azuread_group.data_scientists[0].object_id
}

# Role Assignment: Data Scientists Group - SQL DB Data Reader
resource "azurerm_role_assignment" "data_scientists_sql_reader" {
  count                = length(data.azuread_group.data_scientists) > 0 ? 1 : 0
  scope                = azurerm_mssql_database.main.id
  role_definition_name = "SQL DB Data Reader"
  principal_id         = data.azuread_group.data_scientists[0].object_id
  
  lifecycle {
    create_before_destroy = true
  }
}

# Definición de rol personalizado para operadores de pipeline de datos
resource "azurerm_role_definition" "data_pipeline_operator" {
  name        = "Data Pipeline Operator"
  scope       = azurerm_resource_group.main.id
  description = "Rol personalizado para operadores de pipelines de datos"

  permissions {
    actions = [
      # Data Factory permissions
      "Microsoft.DataFactory/factories/read",
      "Microsoft.DataFactory/factories/write",
      "Microsoft.DataFactory/factories/pipelines/*",
      "Microsoft.DataFactory/factories/datasets/*",
      "Microsoft.DataFactory/factories/linkedservices/*",
      "Microsoft.DataFactory/factories/triggers/*",
      "Microsoft.DataFactory/factories/integrationruntimes/*",
      
      # Storage permissions
      "Microsoft.Storage/storageAccounts/read",
      "Microsoft.Storage/storageAccounts/blobServices/containers/read",
      "Microsoft.Storage/storageAccounts/blobServices/containers/write",
      "Microsoft.Storage/storageAccounts/blobServices/generateUserDelegationKey/action",
      
      # Key Vault permissions
      "Microsoft.KeyVault/vaults/read",
      "Microsoft.KeyVault/vaults/secrets/read",
      
      # Event Hub permissions
      "Microsoft.EventHub/namespaces/read",
      "Microsoft.EventHub/namespaces/eventhubs/read",
      "Microsoft.EventHub/namespaces/eventhubs/write",
      
      # SQL permissions
      "Microsoft.Sql/servers/read",
      "Microsoft.Sql/servers/databases/read",
      "Microsoft.Sql/servers/databases/write"
    ]
    
    not_actions = [
      # Prevent deletion operations
      "*/delete",
      "Microsoft.Authorization/*/Delete",
      "Microsoft.Network/*/Delete",
      "Microsoft.Compute/*/Delete"
    ]
  }

  assignable_scopes = [
    azurerm_resource_group.main.id
  ]
}

# Data source para obtener información de la suscripción
data "azurerm_subscription" "current" {}

# Network Security Group Rules para restringir acceso
resource "azurerm_network_security_rule" "deny_all_inbound" {
  count                       = var.enable_private_endpoints ? 1 : 0
  name                        = "DenyAllInbound"
  priority                    = 4096
  direction                   = "Inbound"
  access                      = "Deny"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "*"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.main.name
  network_security_group_name = azurerm_network_security_group.data_services[0].name
}

# Permitir tráfico desde rangos IP específicos
resource "azurerm_network_security_rule" "allow_trusted_ips" {
  count                       = var.enable_private_endpoints && length(var.allowed_ip_ranges) > 0 ? 1 : 0
  name                        = "AllowTrustedIPs"
  priority                    = 1000
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_ranges     = ["443", "1433", "9093"]
  source_address_prefixes     = var.allowed_ip_ranges
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.main.name
  network_security_group_name = azurerm_network_security_group.data_services[0].name
}

# Diagnostic Settings para auditoría de RBAC
resource "azurerm_monitor_diagnostic_setting" "keyvault_rbac_audit" {
  count              = var.enable_monitoring ? 1 : 0
  name               = "${local.resource_prefix}-kv-rbac-audit"
  target_resource_id = azurerm_key_vault.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main[0].id

  enabled_log {
    category = "AuditEvent"
  }

  enabled_log {
    category = "AzurePolicyEvaluationDetails"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

# Azure Policy Assignment: Require TLS 1.2
resource "azurerm_resource_group_policy_assignment" "require_tls_12" {
  name                 = "${local.resource_prefix}-require-tls-12"
  resource_group_id    = azurerm_resource_group.main.id
  policy_definition_id = "/providers/Microsoft.Authorization/policyDefinitions/404c3081-a854-4457-ae30-26a93ef643f9"
  description          = "Require TLS 1.2 for all services"
  display_name         = "Require TLS 1.2 - ${var.project_name}"

  parameters = jsonencode({
    effect = {
      value = "Audit"
    }
  })
}

# Azure Policy Assignment: Require encryption in transit
resource "azurerm_resource_group_policy_assignment" "require_encryption_transit" {
  name                 = "${local.resource_prefix}-require-encryption-transit"
  resource_group_id    = azurerm_resource_group.main.id
  policy_definition_id = "/providers/Microsoft.Authorization/policyDefinitions/404c3081-a854-4457-ae30-26a93ef643f9"
  description          = "Require encryption in transit for storage accounts"
  display_name         = "Require Encryption in Transit - ${var.project_name}"

  parameters = jsonencode({
    effect = {
      value = "Audit"
    }
  })
}