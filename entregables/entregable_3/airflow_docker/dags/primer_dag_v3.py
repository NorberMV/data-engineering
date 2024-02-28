from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG(
    dag_id='primer_dag_v3',
    start_date=datetime(2024, 2, 19),
    schedule_interval='@daily'
) as dag:

    task1 = BashOperator(
        task_id='primera_tarea',
        bash_command='echo "Hola mundo, esta es nuestra primera tarea!"'
    )

    task2 = BashOperator(
        task_id='print_hello_world',
        bash_command='echo "HelloWorld!"'
    )

    task3 =BashOperator(
        task_id= 'tercera_tarea',
        bash_command='echo "Hola, soy la tarea 3 y sere corrida luego de Tarea 1 al mismo tiempo que Tarea 2"'
    )

task1 >> task2
task2 >> task3