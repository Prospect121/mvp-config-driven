# Workspace (Azure resource)
resource "azurerm_databricks_workspace" "dbw" {
  name                        = "dbw-${var.name}-dev"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = var.region
  sku                         = "standard" # "trial" si está disponible en tu región
  managed_resource_group_name = "rg-${var.name}-dbw-managed"
}

# Provider de Databricks (requiere az login)
provider "databricks" {
  host                        = azurerm_databricks_workspace.dbw.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.dbw.id
}

resource "databricks_job" "payments_job" {
  name = "payments_v1"
  task {
    task_key = "run_pipeline"
    notebook_task {
      notebook_path = "/Repos/<tu_usuario>/mvp-config-driven/databricks/run_payments" # crea el notebook
    }
    existing_cluster_id = databricks_cluster.small.id
  }
}

resource "databricks_cluster" "small" {
  cluster_name            = "mvp-small"
  spark_version           = "13.3.x-scala2.12" # ejemplo
  node_type_id            = "Standard_DS3_v2"
  autotermination_minutes = 15
  num_workers             = 1
}
