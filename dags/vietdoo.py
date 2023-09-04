from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import os

def copy_data():
    print('copying data')


def count_words():
    print('counting words')

default_args = {
    'owner': 'you',
    'start_date': datetime(2023, 8, 31),
    'retries': 1,
}

dag = DAG('word_processing_dag',
          default_args=default_args,
          schedule_interval=None, 
          catchup=False,
          )

copy_task = PythonOperator(
    task_id='copy_data',
    python_callable=copy_data,
    dag=dag,
)

count_task = PythonOperator(
    task_id='count_words',
    python_callable=count_words,
    dag=dag,
)

copy_task >> count_task
