# Azure Event Hub para streaming de datos en tiempo real
# Event Hub Namespace
resource "azurerm_eventhub_namespace" "main" {
  name                = local.eventhub_namespace
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = var.eventhub_sku
  capacity            = var.eventhub_capacity

  # Configuración de seguridad
  minimum_tls_version           = "1.2"
  public_network_access_enabled = var.enable_private_endpoints ? false : true
  local_authentication_enabled  = false  # Usar solo Azure AD
  
  # Configuración de red simplificada
  # Note: IP rules will be configured manually or through Azure Portal if needed
  # The dynamic block was causing variable scoping issues in this Terraform version

  # Identidad administrada
  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}



# Event Hub para telemetría
resource "azurerm_eventhub" "telemetry" {
  name                = "telemetry-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 7
  
  # Configuración de captura a ADLS - Comentado temporalmente
  # capture_description {
  #   enabled  = true
  #   encoding = "Avro"
  #   
  #   destination {
  #     name                = "EventHubArchive.AzureBlockBlob"
  #     archive_name_format = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
  #     blob_container_name = azurerm_storage_container.raw.name
  #     storage_account_id  = azurerm_storage_account.adls.id
  #   }
  #   
  #   interval_in_seconds = 300
  #   size_limit_in_bytes = 314572800
  # }
}

# Event Hub para transacciones
resource "azurerm_eventhub" "transactions" {
  name                = "transaction-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 8
  message_retention   = 7
  
  # Configuración de captura a ADLS - Comentado temporalmente
  # capture_description {
  #   enabled  = true
  #   encoding = "Avro"
  #   
  #   destination {
  #     name                = "EventHubArchive.AzureBlockBlob"
  #     archive_name_format = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
  #     blob_container_name = azurerm_storage_container.raw.name
  #     storage_account_id  = azurerm_storage_account.adls.id
  #   }
  #   
  #   interval_in_seconds = 60
  #   size_limit_in_bytes = 104857600
  # }
}

# Consumer Group para Data Factory
resource "azurerm_eventhub_consumer_group" "data_factory" {
  name                = "data-factory-consumer"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.main.name
}

# Consumer Group para análisis en tiempo real
resource "azurerm_eventhub_consumer_group" "realtime_analytics" {
  name                = "realtime-analytics-consumer"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = azurerm_resource_group.main.name
}

# Regla de autorización para Data Factory
resource "azurerm_eventhub_authorization_rule" "data_factory" {
  name                = "data-factory-auth"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.main.name
  
  listen = true
  send   = false
  manage = false
}

# Regla de autorización para productores
resource "azurerm_eventhub_authorization_rule" "producer" {
  name                = "producer-auth"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = azurerm_resource_group.main.name
  
  listen = false
  send   = true
  manage = false
}

# Asignación de rol para que Event Hub acceda al almacenamiento
resource "azurerm_role_assignment" "eventhub_storage_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_eventhub_namespace.main.identity[0].principal_id
}

# Endpoint privado para Event Hub (opcional)
resource "azurerm_private_endpoint" "eventhub" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${azurerm_eventhub_namespace.main.name}-pe"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = azurerm_subnet.private_endpoints[0].id

  private_service_connection {
    name                           = "${azurerm_eventhub_namespace.main.name}-psc"
    private_connection_resource_id = azurerm_eventhub_namespace.main.id
    subresource_names              = ["namespace"]
    is_manual_connection           = false
  }

  tags = local.common_tags
}

# Zona DNS privada para Event Hub
resource "azurerm_private_dns_zone" "eventhub" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "privatelink.servicebus.windows.net"
  resource_group_name = azurerm_resource_group.main.name

  tags = local.common_tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "eventhub" {
  count                 = var.enable_private_endpoints ? 1 : 0
  name                  = "eventhub-dns-link"
  resource_group_name   = azurerm_resource_group.main.name
  private_dns_zone_name = azurerm_private_dns_zone.eventhub[0].name
  virtual_network_id    = azurerm_virtual_network.main[0].id

  tags = local.common_tags
}

resource "azurerm_private_dns_a_record" "eventhub" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = azurerm_eventhub_namespace.main.name
  zone_name           = azurerm_private_dns_zone.eventhub[0].name
  resource_group_name = azurerm_resource_group.main.name
  ttl                 = 300
  records             = [azurerm_private_endpoint.eventhub[0].private_service_connection[0].private_ip_address]

  tags = local.common_tags
}