terraform {
  required_providers {
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.5.4"
    }
  }
  cloud {
    organization = "Bricks-Corp"

    workspaces {
      name = "mlops-on-databricks-mlflow"
    }
  }
}

provider "databricks" {
}

data "databricks_current_user" "me" {}
data "databricks_node_type" "smallest" {
  local_disk = true
}
data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_notebook" "this" {
  path     = "${data.databricks_current_user.me.home}/Terraform"
  language = "PYTHON"
  content_base64 = base64encode(<<-EOT
    # created from ${abspath(path.module)}
    display(spark.range(10))
    EOT
  )
}

resource "databricks_job" "this" {
  name = "Terraform Demo 2.0 (${data.databricks_current_user.me.alphanumeric})"
  new_cluster {
    num_workers             = 1
    spark_version           = data.databricks_spark_version.latest_lts.id
    node_type_id            = data.databricks_node_type.smallest.id
    autotermination_minutes = 10
    spark_conf = {
      "spark.databricks.cluster.profile" : "singleNode",
      "spark.master" : "local[*, 4]"
    }
  }
  notebook_task {
    notebook_path = databricks_notebook.this.path
  }
  email_notifications {}
}
output "notebook_url" {
  value = databricks_notebook.this.url
}

output "job_url" {
  value = databricks_job.this.url
}
