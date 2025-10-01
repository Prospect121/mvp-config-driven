# Outputs para información importante del despliegue

# Información del grupo de recursos
output "resource_group_name" {
  description = "Nombre del grupo de recursos"
  value       = azurerm_resource_group.main.name
}

output "resource_group_location" {
  description = "Ubicación del grupo de recursos"
  value       = azurerm_resource_group.main.location
}

# Información de almacenamiento
output "storage_account_name" {
  description = "Nombre de la cuenta de almacenamiento ADLS Gen2"
  value       = azurerm_storage_account.adls.name
}

output "storage_account_primary_endpoint" {
  description = "Endpoint primario de la cuenta de almacenamiento"
  value       = azurerm_storage_account.adls.primary_dfs_endpoint
}

# Comentado temporalmente mientras se resuelven problemas de autenticación
# output "storage_containers" {
#   description = "Contenedores creados en ADLS Gen2"
#   value = {
#     raw        = azurerm_storage_container.raw.name
#     bronze     = azurerm_storage_container.bronze.name
#     silver     = azurerm_storage_container.silver.name
#     gold       = azurerm_storage_container.gold.name
#     quarantine = azurerm_storage_container.quarantine.name
#   }
# }

# Información de Key Vault
output "key_vault_name" {
  description = "Nombre del Key Vault"
  value       = azurerm_key_vault.main.name
}

output "key_vault_uri" {
  description = "URI del Key Vault"
  value       = azurerm_key_vault.main.vault_uri
}

# Información de SQL Database
output "sql_server_name" {
  description = "Nombre del SQL Server"
  value       = azurerm_mssql_server.main.name
}

output "sql_server_fqdn" {
  description = "FQDN del SQL Server"
  value       = azurerm_mssql_server.main.fully_qualified_domain_name
}

output "sql_database_name" {
  description = "Nombre de la base de datos SQL"
  value       = azurerm_mssql_database.main.name
}

# Información de Event Hub
output "eventhub_namespace_name" {
  description = "Nombre del namespace de Event Hub"
  value       = azurerm_eventhub_namespace.main.name
}

output "eventhub_namespace_fqdn" {
  description = "FQDN del namespace de Event Hub"
  value       = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net"
}

output "eventhub_names" {
  description = "Nombres de los Event Hubs creados"
  value = {
    telemetry    = azurerm_eventhub.telemetry.name
    transactions = azurerm_eventhub.transactions.name
  }
}

# Información de Data Factory
output "data_factory_name" {
  description = "Nombre del Data Factory"
  value       = azurerm_data_factory.main.name
}

output "data_factory_identity_principal_id" {
  description = "Principal ID de la identidad administrada del Data Factory"
  value       = azurerm_data_factory.main.identity[0].principal_id
}

# Información de red (si está habilitada)
output "virtual_network_name" {
  description = "Nombre de la red virtual"
  value       = var.enable_private_endpoints ? azurerm_virtual_network.main[0].name : null
}

output "subnet_ids" {
  description = "IDs de las subnets creadas"
  value = var.enable_private_endpoints ? {
    data_services     = azurerm_subnet.data_services[0].id
    private_endpoints = azurerm_subnet.private_endpoints[0].id
  } : null
}

# Información de monitoreo (si está habilitado)
output "log_analytics_workspace_id" {
  description = "ID del workspace de Log Analytics"
  value       = var.enable_monitoring ? azurerm_log_analytics_workspace.main[0].id : null
}

output "application_insights_instrumentation_key" {
  description = "Clave de instrumentación de Application Insights"
  value       = var.enable_monitoring ? azurerm_application_insights.main[0].instrumentation_key : null
  sensitive   = true
}

output "application_insights_connection_string" {
  description = "Cadena de conexión de Application Insights"
  value       = var.enable_monitoring ? azurerm_application_insights.main[0].connection_string : null
  sensitive   = true
}

# Cadenas de conexión (sensibles)
output "sql_connection_string" {
  description = "Cadena de conexión de SQL Database (almacenada en Key Vault)"
  value       = "Stored in Key Vault: ${azurerm_key_vault.main.vault_uri}secrets/sql-connection-string/"
  sensitive   = false
}

output "eventhub_connection_string" {
  description = "Cadena de conexión de Event Hub (almacenada en Key Vault)"
  value       = "Stored in Key Vault: ${azurerm_key_vault.main.vault_uri}secrets/eventhub-connection-string/"
  sensitive   = false
}

output "storage_account_key" {
  description = "Clave de la cuenta de almacenamiento (almacenada en Key Vault)"
  value       = "Stored in Key Vault: ${azurerm_key_vault.main.vault_uri}secrets/storage-account-key/"
  sensitive   = false
}

# Información para configuración de aplicaciones
output "azure_config" {
  description = "Configuración para aplicaciones que se conectan a Azure"
  value = {
    tenant_id                = data.azurerm_client_config.current.tenant_id
    subscription_id          = data.azurerm_client_config.current.subscription_id
    resource_group_name      = azurerm_resource_group.main.name
    location                 = azurerm_resource_group.main.location
    key_vault_name          = azurerm_key_vault.main.name
    storage_account_name    = azurerm_storage_account.adls.name
    sql_server_name         = azurerm_mssql_server.main.name
    sql_database_name       = azurerm_mssql_database.main.name
    eventhub_namespace_name = azurerm_eventhub_namespace.main.name
    data_factory_name       = azurerm_data_factory.main.name
  }
}

# URLs de endpoints privados (si están habilitados)
output "private_endpoints" {
  description = "Información de endpoints privados"
  value = var.enable_private_endpoints ? {
    storage_private_ip    = azurerm_private_endpoint.adls[0].private_service_connection[0].private_ip_address
    # sql_private_ip        = azurerm_private_endpoint.sql[0].private_service_connection[0].private_ip_address
    eventhub_private_ip   = azurerm_private_endpoint.eventhub[0].private_service_connection[0].private_ip_address
    keyvault_private_ip   = azurerm_private_endpoint.keyvault[0].private_service_connection[0].private_ip_address
    datafactory_private_ip = azurerm_private_endpoint.datafactory[0].private_service_connection[0].private_ip_address
  } : null
}

# Información de tags aplicados
output "common_tags" {
  description = "Tags comunes aplicados a todos los recursos"
  value       = local.common_tags
}