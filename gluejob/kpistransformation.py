import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, countDistinct, sum, avg, row_number
from pyspark.sql.window import Window
from datetime import datetime

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 Config
S3_BUCKET = "rawdata-store-mwaa"
BASE_PATH = f"s3://{S3_BUCKET}/music-streaming/raw"
TRANSFORMED_PATH = f"s3://{S3_BUCKET}/music-streaming/transformed"

# Archive config
raw_prefix = "music-streaming/raw/"
archive_prefix = "music-streaming/archive/"
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
log_table = dynamodb.Table("processed_files_log")

def archive_stream_files():
    print("Archiving processed stream files...")
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=raw_prefix)

    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith(".csv") and key.startswith("music-streaming/raw/streams"):
            archive_key = key.replace("raw/", "archive/")
            print(f"Moving {key} → {archive_key}")
            s3_client.copy_object(
                Bucket=S3_BUCKET,
                CopySource={'Bucket': S3_BUCKET, 'Key': key},
                Key=archive_key
            )
            s3_client.delete_object(Bucket=S3_BUCKET, Key=key)

    #archive all files 
    # response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=raw_prefix)
    # for obj in response.get("Contents", []):
    #     key = obj["Key"]
    #     if key.endswith(".csv") and "stream" in key:
    #         destination_key = key.replace(raw_prefix, archive_prefix)
    #         print(f"Moving {key} → {destination_key}")
    #         s3_client.copy_object(
    #             Bucket=S3_BUCKET,
    #             CopySource={"Bucket": S3_BUCKET, "Key": key},
    #             Key=destination_key
    #         )
    #         s3_client.delete_object(Bucket=S3_BUCKET, Key=key)

try:
    print("Reading raw files...")
    songs_df = spark.read.option("header", True).csv(f"{BASE_PATH}/songs.csv")
    users_df = spark.read.option("header", True).csv(f"{BASE_PATH}/users.csv")
    streams_df = spark.read.option("header", True).csv(f"{BASE_PATH}/stream*.csv")

    print("Joining and transforming data...")
    joined = streams_df.join(songs_df, "track_id").join(users_df, "user_id")
    daily = joined.withColumn("date", col("listen_time").cast("date"))

    genre_kpi = daily.groupBy("date", "track_genre").agg(
        count("*").alias("listen_count"),
        countDistinct("user_id").alias("unique_listeners"),
        sum("duration_ms").alias("total_listening_time")
    )

    user_listen_time = daily.groupBy("date", "track_genre", "user_id") \
        .agg(sum("duration_ms").alias("user_total_time"))

    avg_per_user = user_listen_time.groupBy("date", "track_genre") \
        .agg(avg("user_total_time").alias("avg_listening_time_per_user"))

    genre_kpi_final = genre_kpi.join(avg_per_user, on=["date", "track_genre"])

    song_counts = daily.groupBy("date", "track_genre", "track_name").count()
    song_window = Window.partitionBy("date", "track_genre").orderBy(col("count").desc())
    top_3_songs = song_counts.withColumn("rank", row_number().over(song_window)).filter(col("rank") <= 3)

    genre_window = Window.partitionBy("date").orderBy(col("listen_count").desc())
    top_5_genres = genre_kpi.select("date", "track_genre", "listen_count") \
        .withColumn("rank", row_number().over(genre_window)) \
        .filter(col("rank") <= 5)

    def log_successful_streams():
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=raw_prefix)
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith(".csv") and key.startswith("music-streaming/raw/streams"):
                file_name = key.split("/")[-1]
                log_table.put_item(Item={
                    "file_name": file_name,
                    "processed_at": datetime.utcnow().isoformat(),
                    "status": "success",
                    "error_message": None
                })
    print("Writing outputs to S3...")
    genre_kpi_final.write.mode("overwrite").partitionBy("date").parquet(f"{TRANSFORMED_PATH}/genre_kpis")
    top_3_songs.write.mode("overwrite").partitionBy("date").parquet(f"{TRANSFORMED_PATH}/top_3_songs")
    top_5_genres.write.mode("overwrite").partitionBy("date").parquet(f"{TRANSFORMED_PATH}/top_5_genres")
    log_successful_streams()
    archive_stream_files()

except Exception as e:
    print(f"Transform failed: {e}")
    raise


job.commit()
