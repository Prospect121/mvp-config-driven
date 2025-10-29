output "workspace_url" {
  description = "Databricks workspace URL used by the deployment."
  value       = local.databricks_host
}

output "databricks_job_url" {
  description = "URL to the prodi smoke Databricks job."
  value       = format("%s/#job/%s", local.databricks_host, databricks_job.prodi_smoke.id)
}

output "storage_account_name" {
  description = "Name of the storage account backing the lakehouse."
  value       = azurerm_storage_account.this.name
}

output "storage_containers" {
  description = "Container names created for RAW/BRONZE/SILVER/GOLD/CHECKPOINTS."
  value = { for name, container in azurerm_storage_container.layers : name => container.name }
}
