from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

from app.upload_bronze import upload_bronze
from app.process_to_silver import to_silver
from app.process_to_gold import age_to_range,to_gold

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 15),
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="upload_bronze",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_1 = PythonOperator(
        task_id="upload_bronze",
        python_callable=upload_bronze
    )
    task_2 = PythonOperator(
        task_id = "to_silver",
        python_callable = to_silver
    )
    task_3 = PythonOperator(
        task_id = "to_Gold",
        python_callable = to_gold
    )
task_1 >> task_2 >> task_3