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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Governance Webhooks for the Model

# COMMAND ----------

from helpers.mlflowWebhooks import add_job_webhook

if(event_text.startswith('pulkit.chadha@databricks') or event_text.startswith("A test webhook")):
  if model_name:
    client.set_registered_model_tag(name=model_name , key="Webhook-"+event[0], value=event_message['webhook_id'])
    
    add_job_webhook(job_id=419653231361562,event="TRANSITION_REQUEST_TO_STAGING_CREATED",model_name=model_name)
    add_job_webhook(job_id=80786931634121,event="TRANSITION_REQUEST_TO_PRODUCTION_CREATED",model_name=model_name)
    add_job_webhook(job_id=680474644831757,event="MODEL_VERSION_CREATED",model_name=model_name)
    
else:
  print('Skipped')
