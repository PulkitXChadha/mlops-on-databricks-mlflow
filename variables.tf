variable "DATABRICKS_HOST" {
  description = "Databricks Workspace URL"
  type        = string
}

variable "DATABRICKS_TOKEN" {
  description = "Databricks PAT Token"
  type        = string
  sensitive   = true
}

variable "DATABRICKS_REPO_HOME" {
  description = "Parent folder were repo will be added"
  type        = string
}