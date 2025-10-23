locals {
  config_files   = fileset(var.local_config_root, "**/*.yml")
  wheel_path     = coalesce(var.local_wheel_path, "${path.root}/../../dist/${var.package_wheel_filename}")
  wheel_blob_name = "libs/${var.package_wheel_filename}"
}

# Subir archivos YAML a ADLS Gen2 usando API de Blobs (compatible con Gen2)
resource "azurerm_storage_blob" "config_files" {
  for_each               = toset(local.config_files)
  name                   = each.value
  storage_account_name   = azurerm_storage_account.adls.name
  storage_container_name = azurerm_storage_data_lake_gen2_filesystem.configs.name
  type                   = "Block"
  source                 = "${var.local_config_root}/${each.value}"
  content_type           = "application/x-yaml"
}

# Subir wheel al filesystem de ADLS Gen2 (para usarlo via abfss://)
resource "azurerm_storage_blob" "wheel_file" {
  name                   = local.wheel_blob_name
  storage_account_name   = azurerm_storage_account.adls.name
  storage_container_name = azurerm_storage_data_lake_gen2_filesystem.configs.name
  type                   = "Block"
  source                 = local.wheel_path
  content_type           = "application/octet-stream"
}

# Subir init script al filesystem de ADLS Gen2 (para usarlo via abfss://)
resource "azurerm_storage_blob" "init_script" {
  name                   = "init/init-copy-runner.sh"
  storage_account_name   = azurerm_storage_account.adls.name
  storage_container_name = azurerm_storage_data_lake_gen2_filesystem.configs.name
  type                   = "Block"
  source                 = "${path.root}/init-copy-runner.sh"
  content_type           = "text/x-shellscript"
}

# Subir payments_v1 al filesystem de ADLS Gen2 (para usarlo via abfss://)
resource "azurerm_storage_blob" "sample_payments_v1" {
  name                   = "raw/payments/2025/09/26/sample.csv"
  storage_account_name   = azurerm_storage_account.adls.name
  storage_container_name = azurerm_storage_data_lake_gen2_filesystem.configs.name
  type                   = "Block"
  source                 = "${path.root}/samples/payments_v1_sample.csv"
  content_type           = "text/csv"
}

# Salidas ABFS/ABFSS
output "abfs_dataset_config" {
  description = "Ruta ABFS al dataset.yml"
  value       = "abfs://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/${var.dataset_config_path}"
}

output "abfs_env_config" {
  description = "Ruta ABFS al env.yml"
  value       = "abfs://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/env.yml"
}

output "abfs_db_config" {
  description = "Ruta ABFS al database.yml"
  value       = "abfs://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/database.yml"
}

output "abfss_wheel_path" {
  description = "Ruta ABFSS al wheel subido"
  value       = "abfss://${azurerm_storage_data_lake_gen2_filesystem.configs.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/${local.wheel_blob_name}"
}