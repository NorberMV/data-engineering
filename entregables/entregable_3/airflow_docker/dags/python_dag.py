from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'NorberMV',
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
}

def aloha():
    """A simple DAG callable."""
    print("Aloha, first Python DAG!")

with DAG(
    default_args=default_args,
    dag_id="My_first_PythonOperator_DAG",
    start_date=datetime(2024, 2, 19),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='aloha',
        python_callable=aloha,
    )

    task1