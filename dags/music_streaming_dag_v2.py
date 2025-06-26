from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import boto3
from datetime import datetime
import json

def branch_on_validation(**kwargs):
    
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(
            Bucket="rawdata-store-mwaa",
            Key="validation_status/last_result.json"
        )
        result = json.loads(response['Body'].read().decode('utf-8'))
        if result.get("validated"):
            return "transform_data"
        else:
            return "skip_etl"
    except Exception as e:
        print(f"Branch decision failed: {e}")
        return "skip_etl"


def log_etl_summary(**kwargs):
    run_id = kwargs['run_id'] if 'run_id' in kwargs else kwargs['ti'].run_id
    start_time = kwargs['ti'].start_date.isoformat()
    end_time = datetime.utcnow().isoformat()
    status = kwargs.get('status', 'success')

    try:
        # Read processed file list from S3
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket="rawdata-store-mwaa", Key="validation_status/processed_files.json")
        processed_files = json.loads(response["Body"].read().decode("utf-8"))
        print(f"Read {len(processed_files)} processed file(s) from S3.")
    except Exception as e:
        print(f"Could not read processed_files from S3: {e}")
        processed_files = []

    try:
        dynamodb = boto3.resource("dynamodb", region_name="eu-north-1")  # ensure region matches
        table = dynamodb.Table("etl_logs")
        response = table.put_item(Item={
            "run_id": run_id,
            "status": status,
            "started_at": start_time,
            "ended_at": end_time,
            "processed_files": processed_files
        })
        print(f"ETL log entry saved to DynamoDB: {response}")
    except Exception as e:
        print(f"Failed to log ETL summary: {e}")


with DAG(
    dag_id="music_streaming_etl_v2",
    default_args={"owner": "data-engineer"},
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["music", "glue", "dynamodb"]
) as dag:

    start = EmptyOperator(task_id="start")

    validate_data = GlueJobOperator(
        task_id="validate_data",
        job_name="s3-rawdata-validation",
        script_args={},
        iam_role_name="GlueJobRole",
        region_name="eu-north-1",
        wait_for_completion=True
    )

    branch_decision = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=branch_on_validation,
        provide_context=True
    )

    skip_etl = EmptyOperator(task_id="skip_etl")

    transform_data = GlueJobOperator(
        task_id="transform_data",
        job_name="kpi-transformation-job",
        script_args={},
        iam_role_name="GlueJobRole",
        region_name="eu-north-1",
        wait_for_completion=True
    )

    ingest_kpis = GlueJobOperator(
        task_id="ingest_dynamo",
        job_name="dynamo-ingestion",
        script_args={},
        iam_role_name="GlueJobRole",
        region_name="eu-north-1",
        wait_for_completion=True
    )

    log_etl = PythonOperator(
        task_id="log_etl_summary",
        python_callable=log_etl_summary,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE
    )

    end = EmptyOperator(task_id="end")

    start >> validate_data >> branch_decision
    branch_decision >> skip_etl >> log_etl >> end
    branch_decision >> transform_data >> ingest_kpis >> log_etl >> end
