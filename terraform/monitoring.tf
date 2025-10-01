# Configuración de monitoreo y observabilidad

# Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "main" {
  count               = var.enable_monitoring ? 1 : 0
  name                = "${var.project_name}-${var.environment}-law"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days

  tags = local.common_tags
}

# Application Insights
resource "azurerm_application_insights" "main" {
  count               = var.enable_monitoring ? 1 : 0
  name                = "${var.project_name}-${var.environment}-ai"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  workspace_id        = azurerm_log_analytics_workspace.main[0].id
  application_type    = "other"

  tags = local.common_tags
}

# Configuración de diagnóstico para Storage Account
resource "azurerm_monitor_diagnostic_setting" "storage" {
  count                      = var.enable_monitoring ? 1 : 0
  name                       = "storage-diagnostics"
  target_resource_id         = azurerm_storage_account.adls.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main[0].id

  # Comentando logs por ahora, solo usando métricas para Storage Account
  # enabled_log {
  #   category = "StorageRead"
  # }
  #
  # enabled_log {
  #   category = "StorageWrite"
  # }
  #
  # enabled_log {
  #   category = "StorageDelete"
  # }

  metric {
    category = "Transaction"
    enabled  = true
  }

  metric {
    category = "Capacity"
    enabled  = true
  }
}

# Configuración de diagnóstico para SQL Database
resource "azurerm_monitor_diagnostic_setting" "sql" {
  count                      = var.enable_monitoring ? 1 : 0
  name                       = "sql-diagnostics"
  target_resource_id         = azurerm_mssql_database.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main[0].id

  enabled_log {
    category = "SQLInsights"
  }

  enabled_log {
    category = "AutomaticTuning"
  }

  enabled_log {
    category = "QueryStoreRuntimeStatistics"
  }

  enabled_log {
    category = "QueryStoreWaitStatistics"
  }

  enabled_log {
    category = "Errors"
  }

  enabled_log {
    category = "DatabaseWaitStatistics"
  }

  enabled_log {
    category = "Timeouts"
  }

  enabled_log {
    category = "Blocks"
  }

  enabled_log {
    category = "Deadlocks"
  }

  metric {
    category = "Basic"
    enabled  = true
  }

  metric {
    category = "InstanceAndAppAdvanced"
    enabled  = true
  }

  metric {
    category = "WorkloadManagement"
    enabled  = true
  }
}

# Comentado temporalmente para despliegue por fases
/*
# Configuración de diagnóstico para Event Hub
resource "azurerm_monitor_diagnostic_setting" "eventhub" {
  count                      = var.enable_monitoring ? 1 : 0
  name                       = "eventhub-diagnostics"
  target_resource_id         = azurerm_eventhub_namespace.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main[0].id

  enabled_log {
    category = "ArchiveLogs"
  }

  enabled_log {
    category = "OperationalLogs"
  }

  enabled_log {
    category = "AutoScaleLogs"
  }

  enabled_log {
    category = "KafkaCoordinatorLogs"
  }

  enabled_log {
    category = "KafkaUserErrorLogs"
  }

  enabled_log {
    category = "EventHubVNetConnectionEvent"
  }

  enabled_log {
    category = "CustomerManagedKeyUserLogs"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}
*/

# Comentado temporalmente para despliegue por fases
/*
# Configuración de diagnóstico para Data Factory
resource "azurerm_monitor_diagnostic_setting" "datafactory" {
  count                      = var.enable_monitoring ? 1 : 0
  name                       = "datafactory-diagnostics"
  target_resource_id         = azurerm_data_factory.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main[0].id

  enabled_log {
    category = "ActivityRuns"
  }

  enabled_log {
    category = "PipelineRuns"
  }

  enabled_log {
    category = "TriggerRuns"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}
*/

# Configuración de diagnóstico para Key Vault
resource "azurerm_monitor_diagnostic_setting" "keyvault" {
  count                      = var.enable_monitoring ? 1 : 0
  name                       = "keyvault-diagnostics"
  target_resource_id         = azurerm_key_vault.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main[0].id

  enabled_log {
    category = "AuditEvent"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

# Alertas para monitoreo proactivo

# Comentado temporalmente para despliegue por fases
/*
# Alerta para errores en Data Factory
resource "azurerm_monitor_metric_alert" "datafactory_failed_runs" {
  count               = var.enable_monitoring ? 1 : 0
  name                = "datafactory-failed-runs"
  resource_group_name = azurerm_resource_group.main.name
  scopes              = [azurerm_data_factory.main.id]
  description         = "Alert when Data Factory pipeline runs fail"
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.DataFactory/factories"
    metric_name      = "PipelineFailedRuns"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  action {
    action_group_id = azurerm_monitor_action_group.main[0].id
  }

  tags = local.common_tags
}
*/

# Alerta para alta utilización de DTU en SQL Database
resource "azurerm_monitor_metric_alert" "sql_high_dtu" {
  count               = var.enable_monitoring ? 1 : 0
  name                = "sql-high-dtu"
  resource_group_name = azurerm_resource_group.main.name
  scopes              = [azurerm_mssql_database.main.id]
  description         = "Alert when SQL Database DTU usage is high"
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.Sql/servers/databases"
    metric_name      = "dtu_consumption_percent"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 80
  }

  action {
    action_group_id = azurerm_monitor_action_group.main[0].id
  }

  tags = local.common_tags
}

# Comentado temporalmente para despliegue por fases
/*
# Alerta para errores en Event Hub
resource "azurerm_monitor_metric_alert" "eventhub_server_errors" {
  count               = var.enable_monitoring ? 1 : 0
  name                = "eventhub-server-errors"
  resource_group_name = azurerm_resource_group.main.name
  scopes              = [azurerm_eventhub_namespace.main.id]
  description         = "Alert when Event Hub has server errors"
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.EventHub/namespaces"
    metric_name      = "ServerErrors"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  action {
    action_group_id = azurerm_monitor_action_group.main[0].id
  }

  tags = local.common_tags
}
*/

# Action Group para notificaciones
resource "azurerm_monitor_action_group" "main" {
  count               = var.enable_monitoring ? 1 : 0
  name                = "${var.project_name}-${var.environment}-ag"
  resource_group_name = azurerm_resource_group.main.name
  short_name          = "dataplatform"

  # Email notification (configurar según necesidades)
  email_receiver {
    name          = "admin"
    email_address = "admin@company.com"  # Cambiar por email real
  }

  # Webhook notification (opcional)
  # webhook_receiver {
  #   name        = "webhook"
  #   service_uri = "https://your-webhook-url.com"
  # }

  tags = local.common_tags
}

# Comentado temporalmente para despliegue por fases
/*
# Dashboard personalizado
resource "azurerm_portal_dashboard" "main" {
  count                = var.enable_monitoring ? 1 : 0
  name                 = "${var.project_name}-${var.environment}-dashboard"
  resource_group_name  = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location
  dashboard_properties = jsonencode({
    lenses = {
      "0" = {
        order = 0
        parts = {
          "0" = {
            position = {
              x = 0
              y = 0
              rowSpan = 4
              colSpan = 6
            }
            metadata = {
              inputs = [
                {
                  name = "resourceTypeMode"
                  isOptional = true
                }
              ]
              type = "Extension/HubsExtension/PartType/MonitorChartPart"
              settings = {
                content = {
                  options = {
                    chart = {
                      metrics = [
                        {
                          resourceMetadata = {
                            id = azurerm_data_factory.main.id
                          }
                          name = "PipelineSucceededRuns"
                          aggregationType = 1
                          namespace = "Microsoft.DataFactory/factories"
                          metricVisualization = {
                            displayName = "Pipeline Succeeded Runs"
                          }
                        }
                      ]
                      title = "Data Factory Pipeline Runs"
                      titleKind = 1
                      visualization = {
                        chartType = 2
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    metadata = {
      model = {
        timeRange = {
          value = {
            relative = {
              duration = 24
              timeUnit = 1
            }
          }
          type = "MsPortalFx.Composition.Configuration.ValueTypes.TimeRange"
        }
      }
    }
  })

  tags = local.common_tags
}
*/