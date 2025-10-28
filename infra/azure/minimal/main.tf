locals {
  target_resource_group = var.create_databricks_workspace ? azurerm_resource_group.this[0].name : var.resource_group_name
  storage_containers    = ["raw", "bronze", "silver", "gold", "checkpoints"]
}

resource "azurerm_resource_group" "this" {
  count    = var.create_databricks_workspace ? 1 : 0
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "this" {
  name                     = var.storage_account_name
  resource_group_name      = local.target_resource_group
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
  allow_blob_public_access = false
  https_traffic_only_enabled = true
  is_hns_enabled           = true
  tags                     = var.tags

  lifecycle {
    prevent_destroy = false
  }
}

resource "azurerm_storage_container" "layers" {
  for_each              = toset(local.storage_containers)
  name                  = each.key
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_databricks_workspace" "this" {
  count               = var.create_databricks_workspace ? 1 : 0
  name                = var.workspace_name
  resource_group_name = local.target_resource_group
  location            = var.location
  sku                 = "standard"
  tags                = var.tags
}

data "azurerm_databricks_workspace" "existing" {
  count = var.create_databricks_workspace || var.databricks_host != "" ? 0 : 1

  name                = var.workspace_name
  resource_group_name = var.resource_group_name
}

resource "databricks_secret_scope" "prodi" {
  name = "prodi-scope"
}

resource "databricks_secret" "storage_account_name" {
  scope        = databricks_secret_scope.prodi.name
  key          = "AZURE_STORAGE_ACCOUNT_NAME"
  string_value = azurerm_storage_account.this.name
}

resource "databricks_secret" "storage_account_key" {
  scope        = databricks_secret_scope.prodi.name
  key          = "AZURE_STORAGE_ACCOUNT_KEY"
  string_value = azurerm_storage_account.this.primary_access_key
}

resource "databricks_notebook" "prodi_smoke" {
  path            = "/Shared/prodi_smoke_test"
  language        = "PYTHON"
  format          = "SOURCE"
  content_base64  = filebase64("${path.module}/../../../notebooks/azure/prodi_smoke_test.py")
  overwrite       = true
  mkdirs          = true
}

resource "databricks_job" "prodi_smoke" {
  name        = "prodi-smoke"
  max_retries = 1

  job_cluster {
    job_cluster_key = "prodi-smoke-cluster"

    new_cluster {
      spark_version = "14.3.x-scala2.12"
      node_type_id  = "Standard_DS3_v2"
      num_workers   = 1

      spark_env_vars = {
        AZURE_STORAGE_ACCOUNT_NAME = "{{secrets/prodi-scope/AZURE_STORAGE_ACCOUNT_NAME}}"
        AZURE_STORAGE_ACCOUNT_KEY  = "{{secrets/prodi-scope/AZURE_STORAGE_ACCOUNT_KEY}}"
        TZ                         = "America/Bogota"
      }
    }
  }

  task {
    task_key       = "prodi-smoke-notebook"
    description    = "Execute RAW→BRONZE→SILVER smoke test"
    job_cluster_key = "prodi-smoke-cluster"

    notebook_task {
      notebook_path = databricks_notebook.prodi_smoke.path
    }

    timeout_seconds = 1800
  }

  depends_on = [
    databricks_secret.storage_account_key,
    databricks_notebook.prodi_smoke
  ]
}
