terraform {
  required_providers {
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.5.4"
    }
  }

  backend "remote" {
    hostname     = "app.terraform.io"
    organization = "Bricks-Corp"
    workspaces {
      prefix = "mlops-on-databricks-mlflow-"
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
  ml                = true
}

resource "databricks_repo" "mlops-on-databricks-mlflow" {
  url  = "https://github.com/PulkitXChadha/mlops-on-databricks-mlflow.git"
  path = "${var.DATABRICKS_REPO_HOME}/Global/mlops-on-databricks-mlflow"
}

resource "databricks_cluster" "MLOps" {
  cluster_name            = "MLOps"
  num_workers             = 0
<<<<<<< HEAD
  autotermination_minutes = 0
=======
  autotermination_minutes = 0
>>>>>>> d023d9980608c3ef18727c4b952ae8804ad4e0d4
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.smallest.id
  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*, 4]"
  }
  custom_tags = {
    ResourceClass = "SingleNode"
  }
  library {
    pypi {
      package = "databricks_registry_webhooks==0.1.1"
      // repo can also be specified here
    }
  }
}

resource "databricks_job" "model_verison_transitioned_to_prod" {
  name                = "MLOps: MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"
  existing_cluster_id = databricks_cluster.MLOps.id
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"
  }
  email_notifications {}
}

resource "databricks_job" "model_verison_transitioned_to_staging" {
  name                = "MLOps: MODEL_VERSION_TRANSITIONED_TO_STAGING"
  existing_cluster_id = databricks_cluster.MLOps.id
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: MODEL_VERSION_TRANSITIONED_TO_STAGING"
  }
  email_notifications {}
}

resource "databricks_job" "transition_requested_to_prod" {
  name                = "MLOps: TRANSITION_REQUEST_TO_PRODUCTION_CREATED"
  existing_cluster_id = databricks_cluster.MLOps.id
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: TRANSITION_REQUEST_TO_PRODUCTION_CREATED"
  }
  email_notifications {}
}

resource "databricks_job" "transition_requested_to_staging" {
  name                = "MLOps: TRANSITION_REQUEST_TO_STAGING_CREATED"
  existing_cluster_id = databricks_cluster.MLOps.id
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: TRANSITION_REQUEST_TO_STAGING_CREATED"
  }
  email_notifications {}
}

resource "databricks_job" "model_verison_created" {
  name                = "MLOps: MODEL_VERSION_CREATED"
  existing_cluster_id = databricks_cluster.MLOps.id
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: MODEL_VERSION_CREATED"
  }
  email_notifications {}
}

resource "databricks_job" "deploy_webhooks" {
  name                = "MLOps: Deploy Webhooks to New Model"
  existing_cluster_id = databricks_cluster.MLOps.id
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: Deploy Webhooks"
    base_parameters = {
      request_to_prod_job_id       = databricks_job.transition_requested_to_prod.id
      request_to_stage_job_id      = databricks_job.transition_requested_to_staging.id
      transitioned_to_prod_job_id  = databricks_job.model_verison_transitioned_to_prod.id
      transitioned_to_stage_job_id = databricks_job.model_verison_transitioned_to_staging.id
      model_verison_job_id         = databricks_job.model_verison_created.id
    }
  }
  email_notifications {}
}

# resource "databricks_mlflow_webhook" "job" {
#   events      = ["REGISTERED_MODEL_CREATED"]
#   description = "Databricks MLFlow registry webhook"
#   status      = "ACTIVE"
#   job_spec {
#     job_id        = databricks_job.deploy_webhooks.id
#     workspace_url = var.DATABRICKS_HOST
#     access_token  = var.DATABRICKS_TOKEN
#   }
# }

resource "databricks_job" "registered_model_creation" {
  name                = "MLOps: REGISTERED_MODEL_CREATED"
  existing_cluster_id = databricks_cluster.MLOps.id
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: REGISTERED_MODEL_CREATED"
    base_parameters = {
      job_id = databricks_job.deploy_webhooks.id
    }
  }
  email_notifications {}
}


output "repo" {
  value = databricks_repo.mlops-on-databricks-mlflow
}
output "registered_model_creation_job" {
  value = databricks_job.registered_model_creation
}
