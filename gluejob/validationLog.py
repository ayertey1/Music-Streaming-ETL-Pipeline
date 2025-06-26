import boto3
import pandas as pd
import sys
import logging
from datetime import datetime
from io import BytesIO
import json

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
        files = [obj['Key'] for obj in response.get('Contents', []) if "streams" in obj['Key'] and obj['Key'].endswith(".csv")]
        logger.info(f"Found {len(files)} streams files: {files}")
        return files
    except Exception as e:
        logger.error(f"Error listing streams files: {e}")
        return []

def is_already_processed(file_name):
    try:
        resp = table.get_item(Key={"file_name": file_name})
        already = "Item" in resp
        logger.info(f"Checked {file_name} → already processed: {already}")
        return already
    except Exception as e:
        logger.warning(f"DynamoDB read failed for {file_name}: {e}")
        return False

def validate_and_log(file_key, expected_columns, log_to_dynamodb=True):
    file_name = file_key.split("/")[-1]
    if not file_name.strip():
        logger.warning(f"Empty file name from key: {file_key}. Skipping.")
        return False

    try:
        logger.info(f"Reading: s3://{s3_bucket}/{file_key}")
        obj = s3.get_object(Bucket=s3_bucket, Key=file_key)
        df = pd.read_csv(BytesIO(obj['Body'].read()))

        logger.info(f"{file_name} → Columns found: {list(df.columns)}")
        logger.info(f"{file_name} → Required columns: {expected_columns}")

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
        logger.info(f"{file_name} validation SUCCESS")
        return True

    except Exception as e:
        logger.exception(f"Validation failed for {file_name}")
        if log_to_dynamodb:
            table.put_item(Item={
                "file_name": file_name,
                "processed_at": datetime.utcnow().isoformat(),
                "status": "failed",
                "error_message": str(e)
            })
        return False

def main():
    validated = False
    processed_files = []

    logger.info("Validating songs.csv and users.csv (no logging to DynamoDB)")
    validate_and_log(f"{raw_prefix}songs.csv", required_columns["songs"], log_to_dynamodb=False)
    validate_and_log(f"{raw_prefix}users.csv", required_columns["users"], log_to_dynamodb=False)

    logger.info("Validating new stream files")
    stream_files = list_stream_files()
    if not stream_files:
        logger.warning("No stream files found in S3.")

    for stream_file in stream_files:
        file_name = stream_file.split("/")[-1]
        if not is_already_processed(file_name):
            logger.info(f"Validating: {file_name}")
            success = validate_and_log(stream_file, required_columns["streams"])
            if success:
                validated = True
                processed_files.append(file_name)
            else:
                logger.warning(f"{file_name} validation FAILED")
        else:
            logger.info(f"{file_name} already processed. Skipping.")
    # Save processed files list to S3 for downstream use
    s3.put_object(
        Bucket=s3_bucket,
        Key="validation_status/processed_files.json",
        Body=json.dumps(processed_files)
    )
    logger.info(f"Wrote {len(processed_files)} processed file(s) to processed_files.json")


    # Signal output
    flag_result = {"validated": validated}
    s3.put_object(
        Bucket=s3_bucket,
        Key="validation_status/last_result.json",
        Body=json.dumps(flag_result)
    )

    if not validated:
        logger.warning("No new valid stream files found.")
        table.put_item(Item={
            "file_name": "no-new-files",
            "processed_at": datetime.utcnow().isoformat(),
            "status": "skipped",
            "error_message": "No new valid stream files available."
        })
        sys.exit(0)

if __name__ == "__main__":
    main()
