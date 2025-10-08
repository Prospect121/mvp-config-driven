# Azure Container Registry para almacenar imágenes Docker
# Archivo: terraform/container_registry.tf

resource "azurerm_container_registry" "main" {
  name                = "${replace(var.project_name, "-", "")}${var.environment}acr001"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "Basic"
  admin_enabled       = true

  # Configuración de red
  public_network_access_enabled = true
  
  # Tags
  tags = merge(var.common_tags, {
    Component = "Container Registry"
    Purpose   = "Docker Images Storage"
  })
}

# Asignación de roles para Data Factory acceder al Container Registry
resource "azurerm_role_assignment" "df_acr_pull" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# Almacenar credenciales del Container Registry en Key Vault
resource "azurerm_key_vault_secret" "acr_server" {
  name         = "acr-server"
  value        = azurerm_container_registry.main.login_server
  key_vault_id = azurerm_key_vault.main.id
  
  depends_on = [azurerm_key_vault_access_policy.terraform]
}

resource "azurerm_key_vault_secret" "acr_username" {
  name         = "acr-username"
  value        = azurerm_container_registry.main.admin_username
  key_vault_id = azurerm_key_vault.main.id
  
  depends_on = [azurerm_key_vault_access_policy.terraform]
}

resource "azurerm_key_vault_secret" "acr_password" {
  name         = "acr-password"
  value        = azurerm_container_registry.main.admin_password
  key_vault_id = azurerm_key_vault.main.id
  
  depends_on = [azurerm_key_vault_access_policy.terraform]
}

# Output para usar en otros módulos
output "container_registry_login_server" {
  description = "Login server URL del Container Registry"
  value       = azurerm_container_registry.main.login_server
}

output "container_registry_name" {
  description = "Nombre del Container Registry"
  value       = azurerm_container_registry.main.name
}

output "container_registry_id" {
  description = "ID del Container Registry"
  value       = azurerm_container_registry.main.id
}