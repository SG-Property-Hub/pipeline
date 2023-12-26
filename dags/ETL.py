import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "ETL",
    default_args = {
        "owner": "etsu",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

extract_to_bronze = SparkSubmitOperator(
    task_id="extract1",
    conn_id="spark-conn",
    application="spark/jobs/load_to_bronze.py",
    dag=dag
)

transform_to_silver = SparkSubmitOperator(
    task_id="transform1",
    conn_id="spark-conn",
    application="spark/jobs/transform_to_silver.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> extract_to_bronze >> transform_to_silver >> end