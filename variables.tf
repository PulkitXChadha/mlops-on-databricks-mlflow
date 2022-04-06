variable "DATABRICKS_HOST" {
  description = "Databricks Workspace URL"
  type        = string
}

variable "DATABRICKS_TOKEN" {
  description = "Databricks PAT Token"
  type        = string
  sensitive   = true
}
