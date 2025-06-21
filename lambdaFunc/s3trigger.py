import boto3
import json
import os
import urllib3
from base64 import b64encode

MWAA_ENV_NAME = os.environ['MWAA_ENV_NAME']
DAG_NAME = os.environ['DAG_NAME']

def lambda_handler(event, context):
    mwaa_client = boto3.client('mwaa')
    env = mwaa_client.get_environment(Name=MWAA_ENV_NAME)
    web_token = mwaa_client.create_cli_token(Name=MWAA_ENV_NAME)['CliToken']
    web_server_hostname = env['Environment']['WebserverUrl']

    dag_run_url = f"https://{web_server_hostname}/aws_mwaa/cli"

    body = f"dags trigger {DAG_NAME}"
    encoded_body = b64encode(body.encode()).decode()

    http = urllib3.PoolManager()
    response = http.request(
        'POST',
        dag_run_url,
        headers={
            "Authorization": f"Bearer {web_token}",
            "Content-Type": "application/json"
        },
        body=json.dumps({"cli": encoded_body})
    )

    print("Response:", response.status, response.data)
    return {
        'statusCode': response.status,
        'body': response.data.decode()
    }
