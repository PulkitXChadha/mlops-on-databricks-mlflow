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
  event_message = json.loads(all_args['event_message'])
else:
  event_message = json.loads(sample_event_message)

print (event_message)
model_name = event_message['model_name']
event_text = event_message['text']
model_version = event_message['version']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import MLFlow Registry Webhook Python Client

# COMMAND ----------

import mlflow
from mlflowAPIWrapper import postComment
from mlflow.tracking import MlflowClient
client = MlflowClient()

# COMMAND ----------

postComment({
  "comment": "This version is great!",
  "name": "pulkits_demo",
  "version": "1"
})

# COMMAND ----------


