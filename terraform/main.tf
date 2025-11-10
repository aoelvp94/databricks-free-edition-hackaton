terraform {
  required_version = ">= 1.6.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

resource "databricks_schema" "bronze" {
  catalog_name = var.catalog_name
  name         = var.bronze_schema_name
  comment      = "Bronze layer with landed and deduplicated data."
}

resource "databricks_schema" "silver" {
  catalog_name = var.catalog_name
  name         = var.silver_schema_name
  comment      = "Silver layer modeled in an entity-relationship structure."
}

resource "databricks_schema" "gold" {
  catalog_name = var.catalog_name
  name         = var.gold_schema_name
  comment      = "Gold layer focused on KPIs and downstream consumers."
}

resource "databricks_pipeline" "bronze" {
  name        = "travel-hacking-bronze"
  catalog     = var.catalog_name
  target      = databricks_schema.bronze.name
  development = true
  serverless  = true
  edition     = "ADVANCED"
  photon      = false

  channel = "PREVIEW"

  configuration = {
    "bundle.layer"        = "bronze"
    "bundle.synthetic_days" = tostring(var.synthetic_days)
  }

  library {
    notebook {
      path = "${var.repo_path}/notebooks/bronze/01_generate_bronze_data"
    }
  }

  depends_on = [
    databricks_schema.bronze
  ]
}

resource "databricks_pipeline" "silver" {
  name        = "travel-hacking-silver"
  catalog     = var.catalog_name
  target      = databricks_schema.silver.name
  development = true
  serverless  = true
  edition     = "ADVANCED"
  photon      = false

  channel = "PREVIEW"

  configuration = {
    "bundle.layer"        = "silver"
    "source.bronze_schema" = databricks_schema.bronze.name
    "source.catalog_name"  = var.catalog_name
  }

  library {
    notebook {
      path = "${var.repo_path}/notebooks/silver/01_build_silver_model"
    }
  }

  depends_on = [
    databricks_schema.silver,
    databricks_pipeline.bronze
  ]
}

resource "databricks_pipeline" "gold" {
  name        = "travel-hacking-gold"
  catalog     = var.catalog_name
  target      = databricks_schema.gold.name
  development = true
  serverless  = true
  edition     = "ADVANCED"
  photon      = false

  channel = "PREVIEW"

  configuration = {
    "bundle.layer"         = "gold"
    "source.silver_schema" = databricks_schema.silver.name
    "source.catalog_name"  = var.catalog_name
  }

  library {
    notebook {
      path = "${var.repo_path}/notebooks/gold/01_publish_gold_kpis"
    }
  }

  depends_on = [
    databricks_schema.gold,
    databricks_pipeline.silver
  ]
}

