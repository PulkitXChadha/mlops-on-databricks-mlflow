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

context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
context = json.loads(context_str)
run_id_obj = context.get('currentRunId', {})
run_id = run_id_obj.get('id', None) if run_id_obj else None
job_id = context.get('tags', {}).get('jobId', None)


model_name = event_message['model_name']
event_text = event_message['text']
model_version = event_message['version']
event = event_message['event']
webhook_id = event_message['webhook_id']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import MLFlow Registry Webhook Python Client

# COMMAND ----------

from helpers.mlflowAPIWrapper import postComment

postComment({
  "comment": f"""[Webhook-{event}]
  {event_message}
  """,
  "name": model_name,
  "version": model_version
})

postComment({
  "comment": f"""[Webhook-{event}]
  JOB RUN ID: job/{job_id}/run/{run_id}
  Before you request a transition to staging please make sure to:
    - Enter an appropriate Description
    - Have an input and output signature for the model
  """,
  "name": model_name,
  "version": model_version
})
