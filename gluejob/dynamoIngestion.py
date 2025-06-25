import boto3
import pandas as pd
import pyarrow.parquet as pq
import os
import sys
import io
import logging
import re
from datetime import datetime
from decimal import Decimal
from awsglue.utils import getResolvedOptions

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS resources
bucket = "rawdata-store-mwaa"
base_path = f"s3://{bucket}/music-streaming/transformed"
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

kpi_table = dynamodb.Table("daily_kpis")
log_table = dynamodb.Table("processed_files_log")

# Helpers
def safe_int(value):
    return 0 if pd.isna(value) else int(value)

def safe_decimal(value):
    return Decimal("0") if pd.isna(value) else Decimal(str(value))

def read_parquet_df(s3_folder_path):
    try:
        bucket_name = s3_folder_path.split("/")[2]
        prefix = "/".join(s3_folder_path.split("/")[3:])
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                logger.info(f"Reading: s3://{bucket_name}/{obj['Key']}")
                obj_response = s3.get_object(Bucket=bucket_name, Key=obj["Key"])
                parquet_data = obj_response["Body"].read()
                table = pq.read_table(io.BytesIO(parquet_data))
                return table.to_pandas()

        logger.warning(f"No parquet files found in: {s3_folder_path}")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Failed to read parquet: {e}")
        return pd.DataFrame()

def get_latest_partition_date(prefix):
    bucket_name = prefix.split('/')[2]
    prefix_path = '/'.join(prefix.split('/')[3:])
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix_path)

    date_folders = []
    for obj in response.get("Contents", []):
        match = re.search(r'date=(\d{4}-\d{2}-\d{2})/?', obj['Key'])
        if match:
            date_folders.append(match.group(1))

    if not date_folders:
        raise ValueError("No partitioned folders found")

    return max(date_folders)

def is_already_processed(date):
    try:
        response = log_table.get_item(Key={"date": date, "data_type": "DAILY_KPI"})
        return "Item" in response
    except Exception as e:
        logger.warning(f"Error checking processed log: {e}")
        return False

def log_processed_date(date):
    try:
        log_table.put_item(Item={"date": date, "data_type": "DAILY_KPI"})
        logger.info(f"Logged processed date: {date}")
    except Exception as e:
        logger.error(f"Failed to log processed date: {e}")

def put_item(item):
    try:
        kpi_table.put_item(Item=item)
    except Exception as e:
        logger.warning(f"Failed to write item: {item}, error: {e}")

# KPI insertion
def insert_genre_kpis(date):
    df = read_parquet_df(f"{base_path}/genre_kpis/date={date}/")
    for _, row in df.iterrows():
        put_item({
            "date": date,
            "data_type": f"GENRE_KPI#{row['track_genre']}",
            "track_genre": row["track_genre"],
            "listen_count": safe_int(row["listen_count"]),
            "unique_listeners": safe_int(row["unique_listeners"]),
            "total_listening_time": safe_int(row["total_listening_time"]),
            "avg_listening_time_per_user": safe_decimal(row["avg_listening_time_per_user"])
        })

def insert_top_3_songs(date):
    df = read_parquet_df(f"{base_path}/top_3_songs/date={date}/")
    for _, row in df.iterrows():
        put_item({
            "date": date,
            "data_type": f"TOP_SONG#{row['track_genre']}#{safe_int(row['rank'])}",
            "track_genre": row["track_genre"],
            "track_name": row["track_name"],
            "rank": safe_int(row["rank"]),
            "listen_count": safe_int(row["count"])
        })

def insert_top_5_genres(date):
    df = read_parquet_df(f"{base_path}/top_5_genres/date={date}/")
    for _, row in df.iterrows():
        put_item({
            "date": date,
            "data_type": f"TOP_GENRE#{safe_int(row['rank'])}",
            "track_genre": row["track_genre"],
            "rank": safe_int(row["rank"]),
            "listen_count": safe_int(row["listen_count"])
        })

# Main entrypoint
def main():
    try:
        args = getResolvedOptions(sys.argv, ['processing_date'])
        date = args['processing_date']
        logger.info(f"Using passed --processing_date: {date}")
    except:
        logger.info("No processing_date provided. Detecting latest from S3.")
        date = get_latest_partition_date(f"s3://{bucket}/music-streaming/transformed/genre_kpis/")
        logger.info(f"Detected latest date: {date}")

    if is_already_processed(date):
        logger.info(f"Date {date} already processed. Skipping.")
        return

    logger.info(f"Processing KPIs for date: {date}")
    insert_genre_kpis(date)
    insert_top_3_songs(date)
    insert_top_5_genres(date)

    log_processed_date(date)
    logger.info("Ingestion complete.")

if __name__ == "__main__":
    main()
