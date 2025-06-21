from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

default_args = {
    "owner": "data-engineer",
    "retries": 1,
    "start_date": days_ago(1)
}

with DAG("music_streaming_pipeline",
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    start = EmptyOperator(task_id="start")

    validate_data = AwsGlueJobOperator(
        task_id="validate_data",
        job_name="validate-music-stream",
        script_location="s3://<your-bucket>/scripts/validate_and_log.py",
        iam_role_name="GlueJobRole",
        region_name="eu-north-1"
    )

    transform_data = AwsGlueJobOperator(
        task_id="transform_data",
        job_name="transform-kpis",
        script_location="s3://<your-bucket>/scripts/genre_kpi_transform.py",
        iam_role_name="GlueJobRole",
        region_name="eu-north-1"
    )

    ingest_dynamo = AwsGlueJobOperator(
        task_id="ingest_to_dynamo",
        job_name="insert-dynamo",
        script_location="s3://<your-bucket>/scripts/dynamo_ingest.py",
        iam_role_name="GlueJobRole",
        region_name="eu-north-1",
        script_args={"--processing_date": "{{ ds }}" }
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> validate_data >> transform_data >> ingest_dynamo >> end
