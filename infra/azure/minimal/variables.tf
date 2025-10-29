variable "resource_group_name" {
  description = "Name of the Azure resource group where resources will be created or located."
  type        = string
}

variable "location" {
  description = "Azure region for the deployment."
  type        = string
}

variable "workspace_name" {
  description = "Name of the Databricks workspace to create or reference."
  type        = string
}

variable "storage_account_name" {
  description = "Globally unique name for the ADLS Gen2 storage account (3-24 lowercase alphanumeric characters)."
  type        = string
  validation {
    condition     = length(var.storage_account_name) >= 3 && length(var.storage_account_name) <= 24
    error_message = "storage_account_name must be between 3 and 24 characters."
  }
  validation {
    condition     = can(regex("^[a-z0-9]+$", var.storage_account_name))
    error_message = "storage_account_name must contain only lowercase letters and numbers."
  }
}

variable "databricks_host" {
  description = "Databricks workspace host (https://<instance>). Provide when reusing an existing workspace."
  type        = string
  default     = ""
}

variable "databricks_token" {
  description = "Databricks personal access token with permissions to manage secrets, notebooks and jobs."
  type        = string
  sensitive   = true
}

variable "create_databricks_workspace" {
  description = "Set to true to provision a new Databricks workspace. When false, an existing workspace is assumed."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Optional tags to apply to Azure resources."
  type        = map(string)
  default     = {}
}
