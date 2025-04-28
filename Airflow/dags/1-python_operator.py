from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Step 2A: Create a simple Python function
def my_python_task():
    print("✅ Hello from my Python Task!")

# Step 2B: Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Step 2C: Define the DAG
with DAG(
    dag_id='my_python_operator_dag',
    description='A simple DAG that runs a Python function',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',  # every 5 minutes
    catchup=False,
    tags=['custom', 'python', 'python_operator'],
) as dag:

    # Step 2D: Define a task using PythonOperator
    run_python_function = PythonOperator(
        task_id='run_my_python_function',
        python_callable=my_python_task,
    )

# Step 2F: Add a comment to explain the DAG
# This DAG runs a Python function every 5 minutes
 