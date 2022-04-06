import mlflow
import json
from mlflow.utils.rest_utils import http_request
from mlflow.tracking import MlflowClient
from databricks_registry_webhooks import RegistryWebhooksClient, JobSpec

# Create a Job webhook
client = MlflowClient()
host_creds = client._tracking_client.store.get_host_creds()

access_token = host_creds.token

def add_job_webhook(job_id,event,model_name):
  job_spec = JobSpec(job_id=job_id, access_token=access_token)
  job_webhook = RegistryWebhooksClient().create_webhook(events=[event], job_spec=job_spec, model_name=model_name)
  client.set_registered_model_tag(name=model_name , key="Webhook-"+event, value=job_webhook.id)
  return job_webhook.id
