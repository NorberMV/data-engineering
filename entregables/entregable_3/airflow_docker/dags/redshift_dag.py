import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from callables import insert_to_redshift
from utils import CREATE_DB_SQL_PATH, FULL_SCHEMA

this, _ = os.path.split(__file__)
# sql_path = os.path.join(this, 'sql', 'create_db.sql')
sql_path = "sql/create_db.sql"

default_args = {
    'owner': 'NorberMV',
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
        default_args=default_args,
        dag_id="DAG_with_Redshift_Connection",
        description="First Redshift Connection DAG.",
        start_date=datetime(2021, 1, 1),
        schedule_interval="0 3 * * *",
        catchup=False,
        tags=['example']
) as dag:
    task_1 = PostgresOperator(
        task_id='setup__create_table',
        postgres_conn_id="redshift_coder",  # Use your Redshift connection ID set in Airflow's UI
        sql= sql_path, #CREATE_DB_SQL_PATH.as_posix(),  # Path to your SQL file
        # params={
        #     "schema": FULL_SCHEMA,
        # },
    )

    task_2 = PythonOperator(
        task_id='insert_data_to_redshift',
        python_callable=insert_to_redshift,
    )

    task_1 >> task_2 # >> task_get_all_table_data >> task_get_with_filter



# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
# from ..callables import insert_to_redshift
# from ..utils import CREATE_DB_SQL_PATH, FULL_SCHEMA
#
#
# default_args = {
#     'owner': 'NorberMV',
#     'retries': 5,
#     'retry_delay': timedelta(minutes=3),
# }
#
#
#
# with DAG(
#     default_args=default_args,
#     dag_id="DAG_with_Redshift_Connection",
#     description="First Redshift Connection DAG.",
#     start_date=datetime(2021, 1, 1),
#     schedule_interval="0 3 * * *",
#     catchup=False,
#     tags=['example']
# ) as dag:
#
#     task_1 = RedshiftSQLOperator(
#         task_id='setup__create_table',
#         redshift_conn_id="redshift_coder",
#         sql=CREATE_DB_SQL_PATH,
#         params={
#             "schema": FULL_SCHEMA,
#         }
#     )
#     # Insert the data
#     task_2 = PythonOperator(
#         task_id='insert_data_to_redshift',
#         python_callable=insert_to_redshift,
#     )

# task_insert_data = RedshiftSQLOperator(
#     task_id='task_insert_data',
#     sql=[
#         "INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
#         "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
#         "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
#         "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
#         "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
#         "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');",
#     ],
# )
# task_get_all_table_data = RedshiftSQLOperator(
#     task_id='task_get_all_table_data', sql="CREATE TABLE more_fruit AS SELECT * FROM fruit;"
# )
# task_get_with_filter = RedshiftSQLOperator(
#     task_id='task_get_with_filter',
#     sql="CREATE TABLE filtered_fruit AS SELECT * FROM fruit WHERE color = '{{ params.color }}';",
#     params={'color': 'Red'},
# )
