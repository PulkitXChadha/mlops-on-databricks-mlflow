# Databricks notebook source
all_args = dbutils.notebook.entry_point.getCurrentBindings()
if all_args:
  job_id = all_args['job_id']

# COMMAND ----------

import mlflow
import json
from mlflow.utils.rest_utils import http_request
from mlflow.tracking import MlflowClient
from databricks_registry_webhooks import RegistryWebhooksClient, JobSpec

# Create a Job webhook
client = MlflowClient()
host_creds = client._tracking_client.store.get_host_creds()

access_token = host_creds.token


job_spec = JobSpec(job_id=job_id, access_token=access_token)
job_webhook = RegistryWebhooksClient().create_webhook(
  events=["REGISTERED_MODEL_CREATED"],
  description ="",
  job_spec=job_spec
)

job_webhook.id

# COMMAND ----------


