# Azure SQL Database con configuración de seguridad optimizada para East US

# SQL Server
resource "azurerm_mssql_server" "main" {
  name                         = local.sql_server_name
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = random_password.sql_admin.result

  tags = local.common_tags
}

# Base de datos principal con configuración optimizada
resource "azurerm_mssql_database" "main" {
  name           = "${var.project_name}-${var.environment}-db"
  server_id      = azurerm_mssql_server.main.id
  collation      = "SQL_Latin1_General_CP1_CI_AS"
  max_size_gb    = 20
  sku_name       = var.sql_database_sku

  tags = local.common_tags
}

# Firewall rule para permitir servicios de Azure
resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# Nota: Las políticas de auditoría de SQL Server han sido eliminadas debido a problemas persistentes en Azure
# - Extended Auditing: InternalServerError (Tracking ID: a1a05ea0-fc0f-4095-a0d3-7b8c1cf9da1f)
# - Security Alert Policy: OperationTimedOut (Enero 2025)
# 
# RECOMENDACIÓN: Configurar auditoría manualmente en Azure Portal cuando Azure resuelva estos problemas