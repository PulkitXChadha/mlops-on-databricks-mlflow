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
  # Configuration options
  # host  = var.DATABRICKS_HOST
  # token = var.DATABRICKS_TOKEN
}
#variable "DATABRICKS_WORKSPACE_URL" {
#  type        = string
#  description = "This is the PAT token"
#}
#variable "DATABRICKS_TOKEN" {
#  type        = string
#  description = "This is the PAT token"
#}

data "databricks_current_user" "me" {}
data "databricks_spark_version" "latest" {}
data "databricks_node_type" "smallest" {
  local_disk = true
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
    num_workers   = 1
    spark_version = data.databricks_spark_version.latest.id
    node_type_id  = data.databricks_node_type.smallest.id
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
