from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from mysql_to_snowflake import migrate_data

default_args = {
    'owner': 'you',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_mysql_to_snowflake',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=True,  # allows backfilling
    tags=["migration"]
) as dag:

    migrate = PythonOperator(
        task_id='migrate_data_task',
        python_callable=migrate_data,
        op_args=['{{ ds }}']  # ds = execution date
    )
