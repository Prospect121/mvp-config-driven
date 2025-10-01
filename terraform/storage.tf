# Azure Data Lake Storage Gen2 con cifrado y seguridad

# Cuenta de almacenamiento para ADLS Gen2
resource "azurerm_storage_account" "adls" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled          = true  # Habilita Data Lake Storage Gen2

  # Configuración de seguridad
  min_tls_version                = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  
  # Cifrado en reposo
  infrastructure_encryption_enabled = true
  
  # Configuración de red
  public_network_access_enabled = var.enable_private_endpoints ? false : true
  
  network_rules {
    default_action             = var.enable_private_endpoints ? "Deny" : "Allow"
    ip_rules                   = var.allowed_ip_ranges
    virtual_network_subnet_ids = var.enable_private_endpoints ? [azurerm_subnet.data_services[0].id] : []
    bypass                     = ["AzureServices"]
  }

  # Cifrado con claves administradas por el cliente
  dynamic "customer_managed_key" {
    for_each = var.enable_customer_managed_keys ? [1] : []
    content {
      key_vault_key_id          = azurerm_key_vault_key.storage_key[0].id
      user_assigned_identity_id = azurerm_user_assigned_identity.storage[0].id
    }
  }

  # Identidad para acceso a Key Vault
  dynamic "identity" {
    for_each = var.enable_customer_managed_keys ? [1] : []
    content {
      type         = "UserAssigned"
      identity_ids = [azurerm_user_assigned_identity.storage[0].id]
    }
  }

  tags = local.common_tags
}

# Contenedores para diferentes capas de datos
# Comentados temporalmente para evitar problemas de autenticación
# resource "azurerm_storage_container" "raw" {
#   name                  = "raw"
#   storage_account_name  = azurerm_storage_account.adls.name
#   container_access_type = "private"
# }

# resource "azurerm_storage_container" "bronze" {
#   name                  = "bronze"
#   storage_account_name  = azurerm_storage_account.adls.name
#   container_access_type = "private"
# }

# resource "azurerm_storage_container" "silver" {
#   name                  = "silver"
#   storage_account_name  = azurerm_storage_account.adls.name
#   container_access_type = "private"
# }

# resource "azurerm_storage_container" "gold" {
#   name                  = "gold"
#   storage_account_name  = azurerm_storage_account.adls.name
#   container_access_type = "private"
# }

# resource "azurerm_storage_container" "quarantine" {
#   name                  = "quarantine"
#   storage_account_name  = azurerm_storage_account.adls.name
#   container_access_type = "private"
# }

# Identidad administrada para el almacenamiento
resource "azurerm_user_assigned_identity" "storage" {
  count               = var.enable_customer_managed_keys ? 1 : 0
  name                = "${var.project_name}-${var.environment}-storage-identity"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  tags = local.common_tags
}

# Clave para cifrado de almacenamiento
resource "azurerm_key_vault_key" "storage_key" {
  count        = var.enable_customer_managed_keys ? 1 : 0
  name         = "storage-encryption-key"
  key_vault_id = azurerm_key_vault.main.id
  key_type     = "RSA"
  key_size     = 2048

  key_opts = [
    "decrypt",
    "encrypt",
    "sign",
    "unwrapKey",
    "verify",
    "wrapKey",
  ]

  depends_on = [azurerm_key_vault_access_policy.terraform]

  tags = local.common_tags
}

# Política de acceso para la identidad del almacenamiento
resource "azurerm_key_vault_access_policy" "storage" {
  count        = var.enable_customer_managed_keys ? 1 : 0
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_user_assigned_identity.storage[0].principal_id

  key_permissions = [
    "Get",
    "UnwrapKey",
    "WrapKey"
  ]
}

# Endpoint privado para ADLS (opcional)
resource "azurerm_private_endpoint" "adls" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${azurerm_storage_account.adls.name}-pe"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = azurerm_subnet.private_endpoints[0].id

  private_service_connection {
    name                           = "${azurerm_storage_account.adls.name}-psc"
    private_connection_resource_id = azurerm_storage_account.adls.id
    subresource_names              = ["dfs"]
    is_manual_connection           = false
  }

  tags = local.common_tags
}

# Zona DNS privada para ADLS
resource "azurerm_private_dns_zone" "adls" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "privatelink.dfs.core.windows.net"
  resource_group_name = azurerm_resource_group.main.name

  tags = local.common_tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "adls" {
  count                 = var.enable_private_endpoints ? 1 : 0
  name                  = "adls-dns-link"
  resource_group_name   = azurerm_resource_group.main.name
  private_dns_zone_name = azurerm_private_dns_zone.adls[0].name
  virtual_network_id    = azurerm_virtual_network.main[0].id

  tags = local.common_tags
}

resource "azurerm_private_dns_a_record" "adls" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = azurerm_storage_account.adls.name
  zone_name           = azurerm_private_dns_zone.adls[0].name
  resource_group_name = azurerm_resource_group.main.name
  ttl                 = 300
  records             = [azurerm_private_endpoint.adls[0].private_service_connection[0].private_ip_address]

  tags = local.common_tags
}