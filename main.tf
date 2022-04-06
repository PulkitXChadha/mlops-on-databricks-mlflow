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
      name = ["mlops-on-databricks-mlflow-AWS","mlops-on-databricks-mlflow-GCP"]
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
  path = "${data.databricks_current_user.me.repos}/mlops-on-databricks-mlflow"
}

resource "databricks_job" "model_verison_transitioned_to_prod" {
  name = "MLOps: MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"
  new_cluster {
    num_workers   = 0
    spark_version = data.databricks_spark_version.latest_lts.id
    node_type_id  = data.databricks_node_type.smallest.id
    spark_conf = {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*, 4]"
    }
    custom_tags = {
      ResourceClass = "SingleNode"
    }
  }
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"
  }
  email_notifications {}
}

resource "databricks_job" "model_verison_transitioned_to_staging" {
  name = "MLOps: MODEL_VERSION_TRANSITIONED_TO_STAGING"
  new_cluster {
    num_workers   = 0
    spark_version = data.databricks_spark_version.latest_lts.id
    node_type_id  = data.databricks_node_type.smallest.id
    spark_conf = {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*, 4]"
    }
    custom_tags = {
      ResourceClass = "SingleNode"
    }
  }
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: MODEL_VERSION_TRANSITIONED_TO_STAGING"
  }
  email_notifications {}
}

resource "databricks_job" "transition_requested_to_prod" {
  name = "MLOps: TRANSITION_REQUEST_TO_PRODUCTION_CREATED"
  new_cluster {
    num_workers   = 0
    spark_version = data.databricks_spark_version.latest_lts.id
    node_type_id  = data.databricks_node_type.smallest.id
    spark_conf = {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*, 4]"
    }
    custom_tags = {
      ResourceClass = "SingleNode"
    }
  }
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: TRANSITION_REQUEST_TO_PRODUCTION_CREATED"
  }
  email_notifications {}
}

resource "databricks_job" "transition_requested_to_staging" {
  name = "MLOps: TRANSITION_REQUEST_TO_STAGING_CREATED"
  new_cluster {
    num_workers   = 0
    spark_version = data.databricks_spark_version.latest_lts.id
    node_type_id  = data.databricks_node_type.smallest.id
    spark_conf = {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*, 4]"
    }
    custom_tags = {
      ResourceClass = "SingleNode"
    }
  }
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: TRANSITION_REQUEST_TO_STAGING_CREATED"
  }
  email_notifications {}
}

resource "databricks_job" "model_verison_created" {
  name = "MLOps: MODEL_VERSION_CREATED"
  new_cluster {
    num_workers   = 0
    spark_version = data.databricks_spark_version.latest_lts.id
    node_type_id  = data.databricks_node_type.smallest.id
    spark_conf = {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*, 4]"
    }
    custom_tags = {
      ResourceClass = "SingleNode"
    }
  }
  notebook_task {
    notebook_path = "${databricks_repo.mlops-on-databricks-mlflow.path}/MLOps: MODEL_VERSION_CREATED"
  }
  email_notifications {}
}

resource "databricks_job" "deploy_webhooks" {
  name = "MLOps: Deploy Webhooks to New Model"
  new_cluster {
    num_workers   = 0
    spark_version = data.databricks_spark_version.latest_lts.id
    node_type_id  = data.databricks_node_type.smallest.id
    spark_conf = {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*, 4]"
    }
    custom_tags = {
      ResourceClass = "SingleNode"
    }
  }
  library {
    pypi {
      package = "databricks_registry_webhooks==0.1.1"
      // repo can also be specified here
    }
  }
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

resource "databricks_job" "registered_model_creation" {
  name = "MLOps: REGISTERED_MODEL_CREATED"
  new_cluster {
    num_workers   = 0
    spark_version = data.databricks_spark_version.latest_lts.id
    node_type_id  = data.databricks_node_type.smallest.id
    spark_conf = {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*, 4]"
    }
    custom_tags = {
      ResourceClass = "SingleNode"
    }
  }
  library {
    pypi {
      package = "databricks_registry_webhooks==0.1.1"
      // repo can also be specified here
    }
  }
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
