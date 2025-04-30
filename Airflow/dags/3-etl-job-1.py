from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl_extract.insert_orders import insert_orders
from etl_extract.connect import connect

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='etl_job_1',
    description='ETL job that inserts dummy data and extracts the daily orders from postgresql , transforms it, and loads it into a target system',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 */2 * * *',  # every 2 hours
    catchup=False,
    tags=['custom', 'etl', 'python', 'python_operator'], 
) as dag:
    #insert dummy data for extraction
    insert_dummy_data = PythonOperator(
    task_id='insert_dummy_data',
    python_callable=insert_orders,
    )