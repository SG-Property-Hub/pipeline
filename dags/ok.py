from airflow import DAG
from datetime import datetime, timedelta
import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Đỗ Quốc Việt',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


def print_scrape_in_progress():
    print('Scraped is in progress!')


def list_dir():
    print(2**22)
    print(os.listdir('.'))
    # list dir /property_crawler
    print(os.listdir(os.getcwd() + '/property_crawler'))


dag = DAG('CRAWL_ha_DEMO', default_args=default_args)

t0 = PythonOperator(
    task_id='list_files',
    python_callable=list_dir,
    dag=dag
)

t2 = PythonOperator(
    task_id='scrape_progress',
    python_callable=print_scrape_in_progress,
    dag=dag)


t0 >> t2
