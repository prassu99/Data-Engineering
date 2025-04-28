from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    dag_id='multiple_tasks_dag',
    description='A simple DAG that runs a  Python function and a Bash command',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',  # every 5 minutes
    catchup=False,
    tags=['custom', 'python', 'python_operator', 'bash', 'bash_operator'], 
) as dag:

    # Step 2D: Define a task using PythonOperator
    run_python_function1 = PythonOperator(
        task_id='run_my_python_function',
        python_callable=my_python_task,
    )

    run_bash_command1 = BashOperator(
        task_id='run_bash_command',
        bash_command='echo "Hello from Bash!"',
    )
    # If you had multiple tasks, you could set dependencies like:
    # run_python_function >> another_task

# Step 2E: Set task dependencies
    run_python_function1 >> run_bash_command1
# Step 2F: Add a comment to explain the DAG
# This DAG runs a Python function and a Bash command every 5 minutes
 