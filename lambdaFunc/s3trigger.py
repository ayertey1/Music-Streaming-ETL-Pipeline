import json
import boto3
import os
import urllib3

def lambda_handler(event, context):
    mwaa_env_name = os.environ['MWAA_ENV_NAME']
    dag_name = os.environ['DAG_NAME']
    
    # Get MWAA CLI Token
    mwaa = boto3.client('mwaa')
    env_info = mwaa.get_environment(Name=mwaa_env_name)
    web_token = mwaa.create_cli_token(Name=mwaa_env_name)
    mwaa_cli_token = web_token['CliToken']
    mwaa_web_server = env_info['Environment']['WebserverUrl']

    # Trigger DAG via Airflow CLI API endpoint
    http = urllib3.PoolManager()
    trigger_command = f"dags trigger {dag_name}"

    response = http.request(
        'POST',
        f'https://{mwaa_web_server}/aws_mwaa/cli',
        headers={
            'Authorization': f'Bearer {mwaa_cli_token}',
            'Content-Type': 'text/plain'
        },
        body=trigger_command.encode()
    )

    print(f"Triggered DAG {dag_name}: {response.data.decode('utf-8')}")
    return {
        'statusCode': 200,
        'body': json.dumps('DAG triggered successfully!')
    }
