variable "databricks_host" {
  type        = string
  description = "Base URL for the Databricks Free Edition workspace (https://...)."
}

variable "databricks_token" {
  type        = string
  description = "Personal Databricks token used for authentication."
  sensitive   = true
}

variable "catalog_name" {
  type        = string
  description = "Unity Catalog name to target."
  default     = "workspace"
}

variable "bronze_schema_name" {
  type        = string
  description = "Schema name for the Bronze layer."
  default     = "bronze_travel"
}

variable "silver_schema_name" {
  type        = string
  description = "Schema name for the Silver layer."
  default     = "silver_travel"
}

variable "gold_schema_name" {
  type        = string
  description = "Schema name for the Gold layer."
  default     = "gold_travel"
}

variable "repo_path" {
  type        = string
  description = "Workspace path to the repository (e.g. /Repos/<user>/databricks-free-edition-hackaton)."
}

variable "spark_version" {
  type        = string
  description = "Serverless runtime version for ML jobs."
  default     = "14.3.x-cpu-ml-scala2.12"
}

variable "synthetic_days" {
  type        = number
  description = "Number of future days to generate in the Bronze pipeline."
  default     = 35
}

