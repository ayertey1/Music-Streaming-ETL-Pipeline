# Music Streaming ETL Pipeline

This project implements a near-real-time ETL pipeline for a music streaming platform using Amazon Web Services (AWS). The pipeline ingests stream data from S3, validates and transforms it using AWS Glue and Apache Airflow (MWAA), and stores key performance indicators (KPIs) into DynamoDB for analytics and monitoring.

---

## Architecture Overview

The pipeline is event-driven and fully serverless, built with the following AWS components:

* **Amazon S3** – Source of incoming stream data and storage for raw, transformed, and archived files
* **AWS Lambda** – Triggered by new S3 uploads to start the DAG in MWAA
* **Amazon MWAA (Airflow)** – Orchestrates the ETL steps as a DAG
* **AWS Glue Jobs** – Performs validation, transformation, and ingestion tasks
* **Amazon DynamoDB** – Stores both KPI metrics and ETL execution logs
* **Amazon CloudWatch Logs** – Captures logs from Airflow and Glue for observability

---

## Components

### 1. **S3 Bucket Layout**

```text
s3://rawdata-store-mwaa/
├── music-streaming/
│   ├── raw/                  # Incoming raw files (songs.csv, users.csv, stream*.csv)
│   ├── archive/              # Archived validated stream files
│   └── transformed/         # Output KPIs in partitioned Parquet
```

### 2. **Glue Jobs**

#### Validation Job (Python Shell)

* Validates required columns in all CSVs
* Logs processed files into `processed_files_log` table in DynamoDB
* Skips DAG execution if no new files are found

#### Transformation Job (Spark)

* Joins songs, users, and stream data
* Computes:

  * Listen Count
  * Unique Listeners
  * Total Listening Time
  * Average Listening Time per User
  * Top 3 Songs per Genre per Day
  * Top 5 Genres per Day
* Writes output to partitioned Parquet in S3
* Archives processed stream files

#### DynamoDB Ingestion Job (Python Shell)

* Reads Parquet outputs
* Inserts KPI data into the `daily_kpis` table
* Logs successful runs into `processed_files_log`

---

## Airflow DAG: `music_streaming_etl_v2`

### DAG Flow:

1. **Start → Validate Data**
2. **Branch: Skip DAG** if no new stream files
3. **Transform Data** with Glue Spark Job
4. **Ingest KPIs** into DynamoDB
5. **Log ETL Run** to `etl_logs` table
6. **End**

### Logging

All ETL runs are logged in DynamoDB `etl_logs` with fields:

* `run_id`, `status`, `started_at`, `ended_at`, `processed_files`

---

## Requirements

```txt
pandas==2.2.2
pyarrow==15.0.2
apache-airflow-providers-amazon>=6.1.0
```

Upload this as `requirements.txt` to your MWAA S3 bucket and link it in the MWAA console.

---

## IAM Policies

Ensure the following permissions are included in the MWAA execution role:

* `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
* `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJob`
* `dynamodb:PutItem`, `dynamodb:GetItem`
* `logs:*`, `cloudwatch:PutMetricData`
* `airflow:PublishMetrics`
* `sqs:*`, `kms:*` (for Celery queues)

---

## Setup Checklist

1. Upload DAG to `s3://<mwaa-bucket>/dags/`
2. Upload `requirements.txt` to root of S3 and link in MWAA console
3. Create Glue Jobs: `validate_music_streams`, `transform_music_kpis`, `dynamo_kpi_ingestion`
4. Create DynamoDB tables: `processed_files_log`, `daily_kpis`, `etl_logs`
5. Deploy Lambda that triggers DAG when new stream file is added to S3
6. Grant MWAA and Lambda roles proper IAM permissions

---

## Example KPIs Tracked

* `Listen Count`: Total streams per genre/day
* `Unique Listeners`: Distinct users per genre/day
* `Total Listening Time`: Sum of duration\_ms
* `Average Listening Time`: Per user per genre/day
* `Top 3 Songs` per genre/day
* `Top 5 Genres` per day

---

