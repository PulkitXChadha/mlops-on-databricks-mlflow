import mlflow
import json
from mlflow.utils.rest_utils import http_request
from mlflow.tracking import MlflowClient

client = MlflowClient()
host_creds = client._tracking_client.store.get_host_creds()


def mlflow_call_endpoint(endpoint, method, body='{}'):
    if method == 'GET':
        response = http_request(host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(
            endpoint), method=method, params=json.loads(body))
    else:
        response = http_request(host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(
            endpoint), method=method, json=json.loads(body))
    return response.json()


def postComment(json_obj):
    mlflow_call_endpoint('comments/create', 'POST', body=json.dumps(json_obj))
    print(f'Comment Posted')

def addTagToVersion(model_name,version,key, value):
    client.set_model_version_tag(name=model_name, version=version, key=key, value=value)
    print(f'tag {key} added to version {version}')
    
def addTagToModel(model_name, key, value):
    client.set_registered_model_tag(name=model_name , key=key, value=value)
    print(f'tag {key} added to model')
  