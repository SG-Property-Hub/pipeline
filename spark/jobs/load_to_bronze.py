from pyspark.sql import SparkSession
import random
import string
import pymongo
import os
import boto3
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO

MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_HOST = os.environ.get('MINIO_HOST')

s3 = boto3.client(
    's3',
    endpoint_url = MINIO_HOST,
    aws_access_key_id = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    region_name='us-east-1'
)

# spark = SparkSession.builder \
#     .appName("Write to Parquet") \
#     .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
#     .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
#     .config("spark.hadoop.fs.s3a.endpoint", MINIO_HOST) \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

MONGO_URL = os.environ.get('MONGO_URL')
MONGO_DB = os.environ.get('MONGO_DB')
MONGO_COLLECTION = os.environ.get('MONGO_COLLECTION')

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

bronze_bucket_name = 'bronze'
if not s3.list_buckets()['Buckets']:
    s3.create_bucket(Bucket = bronze_bucket_name)

today = datetime.now().strftime("%Y-%m-%d")
target_date = '2023-11-06'

bronze_folders = []
try:
    for obj in s3.list_objects(Bucket = bronze_bucket_name)['Contents']:
        bronze_folders.append(obj['Key'])
except Exception as e:
    print(f'Error listing objects: {e}')

print('bronze_folders: ', bronze_folders)

start_date = datetime(2023, 10, 26)
end_date = datetime.now()

LIMIT = 100000

def delete_s3_folder(s3, bucket_name, prefix):
    objects_to_delete = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in objects_to_delete:
        for obj in objects_to_delete['Contents']:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
    s3.delete_object(Bucket=bucket_name, Key=prefix)

current_date = start_date
count_total_processed = 0
while current_date <= end_date:
    query_date = str(current_date.strftime("%Y-%m-%d")) 
    current_date += timedelta(days = 1)

    num_documents = collection.count_documents({'initial_date': query_date})
    if not num_documents:
        continue

    offset = 0
    print(query_date, num_documents)   
    id = 0

    try:
        delete_s3_folder(s3, bronze_bucket_name, f'{query_date}/')
    except Exception as e:
        print(f'Error deleting folder: {e}')

    while offset < num_documents:
        num_this_batch = LIMIT if offset + LIMIT < num_documents else num_documents - offset
        batch_cursor = collection.find({'initial_date': query_date}, {'_id': 0}).skip(offset).limit(LIMIT)
        offset += LIMIT
        
        batch_data = list(batch_cursor)
        print('num_this_batch: ', len(batch_data))
        file_name = f'part-{id}.parquet'

        
        df = pd.DataFrame(batch_data)
        buffer = BytesIO()
        df.to_parquet(buffer, engine='pyarrow')
        buffer.seek(0)
  
        if f'/{query_date}/{file_name}' in bronze_folders:
            s3.delete_object(Bucket = bronze_bucket_name, Key = f'/{query_date}/{file_name}')
        s3.upload_fileobj(buffer, bronze_bucket_name, f'/{query_date}/{file_name}')
        

        print(f'Uploaded to s3: {bronze_bucket_name}/{query_date}/{file_name}')

        id = id + 1


