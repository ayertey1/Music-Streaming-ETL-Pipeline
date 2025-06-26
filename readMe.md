#  Music Streaming ETL Pipeline

This project implements a **production-grade, event-driven ETL pipeline** for a music streaming platform using **Amazon Web Services (AWS)**. It processes user stream data in near real-time and generates business-critical **genre-level KPIs** stored in DynamoDB for downstream analytics, dashboards, and reports.

> Designed with scalability, observability, and modularity in mind using fully managed services.

---

## Architecture Overview

This pipeline leverages a **serverless architecture** composed of the following AWS components:

- **Amazon S3** – Raw stream, song metadata, and user metadata source; also stores transformed outputs
- **AWS Lambda** – Triggers the MWAA DAG when a new stream file lands in S3
- **Amazon MWAA (Airflow)** – Orchestrates the ETL flow using a DAG
- **AWS Glue (Python Shell & Spark Jobs)** – Handles validation, transformation, and ingestion
- **Amazon DynamoDB** – Stores processed KPIs and ETL audit logs
- **Amazon CloudWatch Logs** – Captures logs from MWAA and Glue for centralized monitoring

---

## S3 Directory Layout
```text
s3://rawdata-store-mwaa/
└── music-streaming/
    ├── raw/           # Input data (songs.csv, users.csv, stream*.csv)
    ├── transformed/   # Output KPIs in partitioned Parquet
    └── archive/       # Archived/processed stream files
```
##  Pipeline Components
---

### 1. **Validation Job** (`validate_music_streams` – Python Shell)

* Checks for required columns in:

  * `songs.csv` (e.g., track\_id, track\_genre)
  * `users.csv` (e.g., user\_id, created\_at)
  * All incoming `stream*.csv` files
* Uses `processed_files_log` in **DynamoDB** to avoid reprocessing
* Skips the pipeline if no new stream files are found
* Logs all results (pass/fail/skip)

---

### 2. **Transformation Job** (`transform_music_kpis` – Spark)

* Joins raw `streams`, `songs`, and `users`
* Calculates daily genre-level and track-level KPIs:

  * `listen_count`, `unique_listeners`, `total_listening_time`, `avg_listening_time_per_user`
  * `Top 3 Songs` per genre/day
  * `Top 5 Genres` per day
* Writes results to **partitioned Parquet** files in S3
* Archives processed stream files to `archive/`
* Logs successfully processed files to DynamoDB

---

### 3. **Ingestion Job** (`dynamo_kpi_ingestion` – Python Shell)

* Reads Parquet outputs from `transformed/`
* Inserts metrics into `daily_kpis` table in DynamoDB:

  * Partition key: `date`
  * Sort key: `data_type` (e.g., `GENRE_KPI#Pop`)
* Supports safe ingestion with `Decimal` handling and deduplication
* Marks successful ingestion in `processed_files_log`

---

## DAG Overview: `music_streaming_etl_v2`

### Orchestrated by MWAA

## DAG Flow:

```text
start
  └─> validate_data (Glue)
         └─> branch_decision
              ├─> skip_etl
              └─> transform_data (Glue)
                     └─> ingest_dynamo (Glue)
                             └─> log_etl_summary
```

## Task Descriptions

| Task              | Description                                           |
| ----------------- | ----------------------------------------------------- |
| `validate_data`   | Glue job to validate file structure and log new files |
| `branch_decision` | Skips rest of DAG if no new files                     |
| `transform_data`  | Spark job to compute KPIs and archive data            |
| `ingest_dynamo`   | Loads KPIs into DynamoDB                              |
| `log_etl_summary` | Writes audit log entry to `etl_logs` table            |

## `etl_logs` Schema

| Field             | Type   | Description                       |
| ----------------- | ------ | --------------------------------- |
| `run_id`          | String | Unique Airflow DAG run identifier |
| `status`          | String | success, failed, or skipped       |
| `started_at`      | String | ISO timestamp of run start        |
| `ended_at`        | String | ISO timestamp of run end          |
| `processed_files` | List   | Names of validated stream files   |

---

## Example KPIs Computed

| Metric                 | Description                               |
| ---------------------- | ----------------------------------------- |
| `Listen Count`         | Number of streams per genre/day           |
| `Unique Listeners`     | Distinct users per genre/day              |
| `Total Listening Time` | Sum of `duration_ms` across all streams   |
| `Avg Listening Time`   | Average listening time per user/genre/day |
| `Top 3 Songs`          | Most streamed tracks per genre/day        |
| `Top 5 Genres`         | Most streamed genres per day              |

---

## IAM Requirements

### MWAA Execution Role Must Have:

```json
{
  "Effect": "Allow",
  "Action": [
    "airflow:*",
    "glue:StartJobRun",
    "glue:GetJob*",
    "dynamodb:*",
    "logs:*",
    "cloudwatch:*",
    "s3:*"
  ],
  "Resource": "*"
}
```

### Lambda Role (for triggering MWAA):

* `airflow:GetEnvironment`, `airflow:CreateCliToken`
* `s3:GetObject` (for event data if needed)

---

## Setup Checklist

1. Upload DAG to: `s3://<MWAA-bucket>/dags/`
2. Upload `requirements.txt` and link in MWAA
3. Create three AWS Glue jobs:

   * `validate_music_streams` (Python Shell)
   * `transform_music_kpis` (Spark)
   * `dynamo_kpi_ingestion` (Python Shell)
4. Create DynamoDB tables:

   * `processed_files_log`
   * `daily_kpis`
   * `etl_logs`
5. Deploy Lambda function triggered on new S3 upload to `raw/streams/`
6. Assign correct IAM policies to MWAA and Lambda roles

---

## Dependencies

These must be included in your MWAA environment’s `requirements.txt`:

```text
pandas==2.2.2
pyarrow==15.0.2
apache-airflow-providers-amazon>=6.1.0
```

---

## Monitoring

| Layer      | Logs Captured In          |
| ---------- | ------------------------- |
| Airflow    | Amazon CloudWatch Logs    |
| Glue       | AWS Glue Job Logs         |
| Lambda     | CloudWatch Logs           |
| Audit Logs | DynamoDB `etl_logs` table |

---


