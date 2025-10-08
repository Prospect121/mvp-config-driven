# Azure Container Instances para procesamiento de datos con Python
# Este archivo contiene la configuración para ejecutar el código Python en ACI

# Identidad administrada para Container Instances
resource "azurerm_user_assigned_identity" "aci" {
  name                = "${var.project_name}-${var.environment}-aci-identity"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  tags = local.common_tags
}

# Nota: Container Group se creará dinámicamente desde Data Factory
# No necesitamos un container group permanente

# Storage Share para logs
resource "azurerm_storage_share" "logs" {
  name                 = "logs"
  storage_account_name = azurerm_storage_account.adls.name
  quota                = 50

  depends_on = [azurerm_storage_account.adls]
}

# Storage Share para archivos temporales
resource "azurerm_storage_share" "temp" {
  name                 = "temp"
  storage_account_name = azurerm_storage_account.adls.name
  quota                = 100

  depends_on = [azurerm_storage_account.adls]
}

# Asignaciones de rol para Container Instances
resource "azurerm_role_assignment" "aci_storage_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.aci.principal_id
}

resource "azurerm_role_assignment" "aci_storage_queue_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_user_assigned_identity.aci.principal_id
}

resource "azurerm_role_assignment" "aci_keyvault_reader" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.aci.principal_id
}

resource "azurerm_role_assignment" "aci_sql_contributor" {
  scope                = azurerm_mssql_server.main.id
  role_definition_name = "SQL DB Contributor"
  principal_id         = azurerm_user_assigned_identity.aci.principal_id
}

# Nota: Por simplicidad, usaremos actividades nativas de Data Factory
# en lugar de Custom Activities con ACI por ahora

# Dataset para archivos procesados por Python
resource "azurerm_data_factory_dataset_parquet" "processed_source" {
  name                = "ProcessedSource"
  data_factory_id     = azurerm_data_factory.main.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.adls.name

  azure_blob_storage_location {
    container = azurerm_storage_container.silver.name
    path      = "processed"
  }

  compression_codec = "snappy"
}

# Dataset para logs de procesamiento
resource "azurerm_data_factory_dataset_json" "processing_logs" {
  name                = "ProcessingLogs"
  data_factory_id     = azurerm_data_factory.main.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.adls.name

  azure_blob_storage_location {
    container = azurerm_storage_container.logs.name
    path      = "aci-processing"
    filename  = "processing.log"
  }

  encoding = "UTF-8"
}

# Outputs para referencia
output "aci_identity_principal_id" {
  description = "Principal ID de la identidad administrada de ACI"
  value       = azurerm_user_assigned_identity.aci.principal_id
}

output "aci_identity_client_id" {
  description = "Client ID de la identidad administrada de ACI"
  value       = azurerm_user_assigned_identity.aci.client_id
}