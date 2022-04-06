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

model_name = event_message['model_name']
event_text = event_message['text']

request_to_prod_job_id = all_args['request_to_prod_job_id']
request_to_stage_job_id = all_args['request_to_stage_job_id']
transitioned_to_prod_job_id = all_args['transitioned_to_prod_job_id']
transitioned_to_stage_job_id = all_args['transitioned_to_stage_job_id']
model_verison_job_id = all_args['model_verison_job_id']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Governance Webhooks for the Model

# COMMAND ----------

from mlflow.tracking import MlflowClient
from databricks_registry_webhooks import RegistryWebhooksClient, JobSpec
from helpers.mlflowWebhooks import add_job_webhook
from helpers.mlflowAPIWrapper import addTagToModel

if(event_text.startswith('pulkit.chadha@databricks') or event_text.startswith("A test webhook")):
  if model_name:
    addTagToModel (model_name, "Webhook-"+event_message['event'], event_message['webhook_id'])
    
    add_job_webhook(job_id=transitioned_to_stage_job_id,event="MODEL_VERSION_TRANSITIONED_TO_STAGING",model_name=model_name)
    add_job_webhook(job_id=transitioned_to_prod_job_id,event="MODEL_VERSION_TRANSITIONED_TO_PRODUCTION",model_name=model_name)
    add_job_webhook(job_id=request_to_stage_job_id,event="TRANSITION_REQUEST_TO_STAGING_CREATED",model_name=model_name)
    add_job_webhook(job_id=request_to_prod_job_id,event="TRANSITION_REQUEST_TO_PRODUCTION_CREATED",model_name=model_name)
    add_job_webhook(job_id=model_verison_job_id,event="MODEL_VERSION_CREATED",model_name=model_name)
    
else:
  print('Skipped')
