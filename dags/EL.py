
from airflow import DAG
from datetime import datetime, timedelta
import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pymongo import MongoClient
import boto3
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('spark_example', default_args=default_args,
          schedule_interval=timedelta(days=1))




spark_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/spark/jobs/test.py',  # Đường dẫn tới ứng dụng Spark của bạn
    conn_id='129',  # ID của kết nối Spark trong Airflow
    total_executor_cores='2',  # Số lõi executor bạn muốn sử dụng
    executor_cores='1',  # Số lõi mỗi executor
    executor_memory='2g',  # Bộ nhớ mỗi executor
    dag=dag,
)


spark_task



# def print_scrape_in_progress():
#     print('Scraped is in progress!')

# def list_dir():
#     s3 = boto3.client(
#         's3',
#         aws_access_key_id='BN04Fa38pu9vZw9xDenn',
#         aws_secret_access_key='UGtFioEqETTbKzV66W7se9LqYRo3GUfwnTRZFDeE',
#         endpoint_url='http://minio:9000',
#         verify=False 
#     )
#     current_date = datetime.now().strftime("%Y-%m-%d")
#     bucket_name = f"silver"

#     s3.create_bucket(Bucket=bucket_name)

# dag = DAG('EL_Demo', default_args=default_args)


# t0 = PythonOperator(
#     task_id='list_files',
#     python_callable=list_dir,
#     dag=dag
# )

# t2 = PythonOperator(
#     task_id='scrape_progress',
#     python_callable=print_scrape_in_progress,
#     dag=dag)


# t0 >> t2
