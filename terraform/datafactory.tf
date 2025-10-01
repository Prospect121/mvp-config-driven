# Azure Data Factory para orquestación de pipelines
# Data Factory
resource "azurerm_data_factory" "main" {
  name                = local.data_factory_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  # Configuración de seguridad
  public_network_enabled = var.enable_private_endpoints ? false : true
  
  # Identidad administrada
  dynamic "identity" {
    for_each = var.data_factory_managed_identity ? [1] : []
    content {
      type = "SystemAssigned"
    }
  }

  # Configuración de Git (opcional)
  # github_configuration {
  #   account_name    = "your-github-account"
  #   branch_name     = "main"
  #   git_url         = "https://github.com"
  #   repository_name = "your-repo"
  #   root_folder     = "/datafactory"
  # }

  tags = local.common_tags
}

# Linked Service para Azure Data Lake Storage
resource "azurerm_data_factory_linked_service_azure_blob_storage" "adls" {
  name            = "AzureDataLakeStorage"
  data_factory_id = azurerm_data_factory.main.id
  
  # Usar identidad administrada para autenticación
  use_managed_identity = true
  service_endpoint     = azurerm_storage_account.adls.primary_blob_endpoint
}

# Linked Service para SQL Database
resource "azurerm_data_factory_linked_service_sql_server" "sql" {
  name            = "AzureSQLDatabase"
  data_factory_id = azurerm_data_factory.main.id
  
  key_vault_connection_string {
    linked_service_name = azurerm_data_factory_linked_service_key_vault.main.name
    secret_name         = "sql-connection-string"
  }
}

# Linked Service para Key Vault
resource "azurerm_data_factory_linked_service_key_vault" "main" {
  name            = "AzureKeyVault"
  data_factory_id = azurerm_data_factory.main.id
  key_vault_id    = azurerm_key_vault.main.id
}

# Linked Service para Event Hub
resource "azurerm_data_factory_linked_service_azure_function" "eventhub" {
  name            = "AzureEventHub"
  data_factory_id = azurerm_data_factory.main.id
  url             = "https://${azurerm_eventhub_namespace.main.name}.servicebus.windows.net"
  
  key_vault_key {
    linked_service_name = azurerm_data_factory_linked_service_key_vault.main.name
    secret_name         = "eventhub-connection-string"
  }
}

# Dataset para archivos CSV de entrada - Comentado temporalmente
# resource "azurerm_data_factory_dataset_delimited_text" "csv_source" {
#   name                = "CsvSourceDataset"
#   data_factory_id     = azurerm_data_factory.main.id
#   linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.adls.name

#   azure_blob_storage_location {
#     container = azurerm_storage_container.raw.name
#     path      = "csv"
#     filename  = "*.csv"
#   }

#   column_delimiter    = ","
#   row_delimiter       = "\n"
#   quote_character     = "\""
#   escape_character    = "\\"
#   first_row_as_header = true
#   null_value          = ""
# }

# Dataset para archivos Parquet en ADLS - Comentado temporalmente
# resource "azurerm_data_factory_dataset_parquet" "parquet_sink" {
#   name                = "ParquetSink"
#   data_factory_id     = azurerm_data_factory.main.id
#   linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.adls.name

#   azure_blob_storage_location {
#     container = azurerm_storage_container.silver.name
#     path      = "processed"
#   }

#   compression_codec = "snappy"
# }

# Dataset para SQL Database
resource "azurerm_data_factory_dataset_sql_server_table" "sql_table" {
  name                = "SQLTable"
  data_factory_id     = azurerm_data_factory.main.id
  linked_service_name = azurerm_data_factory_linked_service_sql_server.sql.name
  table_name          = "ProcessedData"
}

# Pipeline principal de procesamiento - Comentado temporalmente
# resource "azurerm_data_factory_pipeline" "main_processing" {
#   name            = "MainProcessingPipeline"
#   data_factory_id = azurerm_data_factory.main.id
#   description     = "Pipeline principal para procesamiento de datos"

#   activities_json = jsonencode([
#     {
#       name = "CopyFromCSVToParquet"
#       type = "Copy"
#       typeProperties = {
#         source = {
#           type = "DelimitedTextSource"
#           storeSettings = {
#             type = "AzureBlobStorageReadSettings"
#             recursive = true
#             wildcardFileName = "*.csv"
#           }
#         }
#         sink = {
#           type = "ParquetSink"
#           storeSettings = {
#             type = "AzureBlobStorageWriteSettings"
#           }
#         }
#         enableStaging = false
#       }
#       inputs = [
#         {
#           referenceName = azurerm_data_factory_dataset_delimited_text.csv_source.name
#           type = "DatasetReference"
#         }
#       ]
#       outputs = [
#         {
#           referenceName = azurerm_data_factory_dataset_parquet.parquet_sink.name
#           type = "DatasetReference"
#         }
#       ]
#     },
#     {
#       name = "ExecuteSparkJob"
#       type = "SparkJob"
#       dependsOn = [
#         {
#           activity = "CopyFromCSVToParquet"
#           dependencyConditions = ["Succeeded"]
#         }
#       ]
#       typeProperties = {
#         sparkJobLinkedService = {
#           referenceName = "SparkLinkedService"
#           type = "LinkedServiceReference"
#         }
#         rootPath = "abfss://${azurerm_storage_container.silver.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
#         entryFilePath = "spark_jobs/spark_pipeline.py"
#         sparkConfig = {
#           "spark.sql.adaptive.enabled" = "true"
#           "spark.sql.adaptive.coalescePartitions.enabled" = "true"
#         }
#       }
#     }
#   ])

#   parameters = {
#     inputPath  = "raw/csv"
#     outputPath = "silver/processed"
#   }
# }

# Trigger para ejecutar el pipeline diariamente - Comentado temporalmente
# resource "azurerm_data_factory_trigger_schedule" "daily" {
#   name            = "DailyTrigger"
#   data_factory_id = azurerm_data_factory.main.id
#   pipeline_name   = azurerm_data_factory_pipeline.main_processing.name

#   frequency = "Day"
#   interval  = 1
  
#   schedule {
#     hours   = [2]
#     minutes = [0]
#   }

#   activated = true
# }

# Asignaciones de rol para Data Factory
resource "azurerm_role_assignment" "df_storage_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

resource "azurerm_role_assignment" "df_sql_contributor" {
  scope                = azurerm_mssql_database.main.id
  role_definition_name = "SQL DB Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# Endpoint privado para Data Factory (opcional)
resource "azurerm_private_endpoint" "datafactory" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${azurerm_data_factory.main.name}-pe"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = azurerm_subnet.private_endpoints[0].id

  private_service_connection {
    name                           = "${azurerm_data_factory.main.name}-psc"
    private_connection_resource_id = azurerm_data_factory.main.id
    subresource_names              = ["dataFactory"]
    is_manual_connection           = false
  }

  tags = local.common_tags
}

# Zona DNS privada para Data Factory
resource "azurerm_private_dns_zone" "datafactory" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "privatelink.datafactory.azure.net"
  resource_group_name = azurerm_resource_group.main.name

  tags = local.common_tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "datafactory" {
  count                 = var.enable_private_endpoints ? 1 : 0
  name                  = "datafactory-dns-link"
  resource_group_name   = azurerm_resource_group.main.name
  private_dns_zone_name = azurerm_private_dns_zone.datafactory[0].name
  virtual_network_id    = azurerm_virtual_network.main[0].id

  tags = local.common_tags
}

resource "azurerm_private_dns_a_record" "datafactory" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = azurerm_data_factory.main.name
  zone_name           = azurerm_private_dns_zone.datafactory[0].name
  resource_group_name = azurerm_resource_group.main.name
  ttl                 = 300
  records             = [azurerm_private_endpoint.datafactory[0].private_service_connection[0].private_ip_address]

  tags = local.common_tags
}