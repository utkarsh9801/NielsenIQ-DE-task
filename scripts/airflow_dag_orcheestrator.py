from airflow import DAG
from airflow.cli.commands.standalone_command import standalone
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,  # Ensures this run waits for the previous run to succeed
    'wait_for_downstream': True,  # Ensures downstream tasks (if any) are completed
    'start_date': datetime(2025, 3, 13),  # Start date of the DAG
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'movie_ratings_etl',
    default_args=default_args,
    description='DAG to run Movie Ratings ETL process',
    schedule='@daily',  # Runs daily
    catchup=False  # Prevents backfilling of missed runs
)

# Define task to execute ETL script
etl_task = BashOperator(
    task_id='run_etl',
    bash_command='python ETL.pys',
    dag=dag
)

etl_task
print("DAG script completed.")
print(input("Press Enter to continue..."))
