# Databricks notebook source
# MAGIC %md
# MAGIC # MLOps: Deploy Governance Webhooks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse event payload

# COMMAND ----------

import json

sample_event_message = """
 {
   "event":"REGISTERED_MODEL_CREATED",
   "event_timestamp":1648760686678,
   "text":"A test webhook trigger for registered model '' created for webhook bd8414ed622b4a7380b71bf676b04010.",
   "model_name":"",
   "webhook_id":"bd8414ed622b4a7380b71bf676b04010"
 }
"""

all_args = dbutils.notebook.entry_point.getCurrentBindings()

if all_args:
  event_message = json.loads(all_args.event_message)
else:
  event_message = json.loads(sample_event_message)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import MLFlow Registry Webhook Python Client

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient
from databricks_registry_webhooks import RegistryWebhooksClient, JobSpec, HttpUrlSpec

client = MlflowClient()
