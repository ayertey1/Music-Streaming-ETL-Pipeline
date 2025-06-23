import boto3
import pandas as pd
import sys
import logging
from datetime import datetime
from io import BytesIO


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
s3_bucket = "rawdata-store-mwaa"
raw_prefix = "music-streaming/raw/"
required_columns = {
    "songs": ["track_id", "track_genre", "duration_ms", "track_name"],
    "users": ["user_id", "user_name", "user_age", "user_country", "created_at"],
    "streams": ["user_id", "track_id", "listen_time"]
}


# AWS clients
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("processed_files_log")
s3 = boto3.client("s3")

def list_stream_files():
    try:
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=raw_prefix)
        return [obj['Key'] for obj in response.get('Contents', []) if "stream" in obj['Key']]
    except Exception as e:
        logger.error(f"Error listing stream files: {e}")
        return []

def is_already_processed(file_name):
    try:
        resp = table.get_item(Key={"file_name": file_name})
        return "Item" in resp
    except Exception as e:
        logger.warning(f"DynamoDB read failed for {file_name}: {e}")
        return False

def validate_and_log(file_key, expected_columns, log_to_dynamodb=True):
    from io import BytesIO
    file_name = file_key.split("/")[-1]
    if not file_name.strip():
        logger.warning(f"Empty file name from key: {file_key}. Skipping.")
        return False

    try:
        logger.info(f"Reading: s3://{s3_bucket}/{file_key}")
        obj = s3.get_object(Bucket=s3_bucket, Key=file_key)
        df = pd.read_csv(BytesIO(obj['Body'].read()))

        logger.info(f"{file_name} columns: {list(df.columns)}")

        missing = set(expected_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        if log_to_dynamodb:
            table.put_item(Item={
                "file_name": file_name,
                "processed_at": datetime.utcnow().isoformat(),
                "status": "success",
                "error_message": None
            })
        return True

    except Exception as e:
        logger.error(f"Validation failed for {file_name}: {type(e).__name__}: {e}")
        if log_to_dynamodb:
            table.put_item(Item={
                "file_name": file_name,
                "processed_at": datetime.utcnow().isoformat(),
                "status": "failed",
                "error_message": "File structure invalid or unreadable."
            })
        return False

def main():
    validated = False

    logger.info("Validating songs.csv and users.csv (no logging to DynamoDB)")
    validate_and_log(f"{raw_prefix}songs.csv", required_columns["songs"], log_to_dynamodb=False)
    validate_and_log(f"{raw_prefix}users.csv", required_columns["users"], log_to_dynamodb=False)

    logger.info("Validating new stream files")
    for stream_file in list_stream_files():
        file_name = stream_file.split("/")[-1]
        if not is_already_processed(file_name):
            logger.info(f"Validating new stream file: {file_name}")
            if validate_and_log(stream_file, required_columns["streams"]):
                validated = True
        else:
            logger.info(f"{file_name} already processed. Skipping.")

    if not validated:
        logger.warning("No new valid stream files found.")
        table.put_item(Item={
            "file_name": "no-new-files",
            "processed_at": datetime.utcnow().isoformat(),
            "status": "skipped",
            "error_message": "No new valid stream files available."
        })
        sys.exit(0)