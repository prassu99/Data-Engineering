from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl_extract.insert_orders import insert_orders
from etl_extract.extract_orders import extract_orders
from etl_transform.transform_orders import transform_orders
from etl_load.load_orders import load_orders

def etl_job():
    print("Starting ETL job...")
    # This function is a placeholder for the ETL job
    # It can be used to define any additional logic or steps needed in the ETL process
    df = extract_orders()
    df = transform_orders(df)
    df = load_orders(df)
    print("ETL job completed successfully.")

    pass

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
    schedule_interval='0 0 * * *',  # every day at midnight
    catchup=False,
    tags=['custom', 'etl', 'python', 'python_operator'], 
) as dag:
    
    #insert dummy data for extraction
    insert_dummy_data = PythonOperator(
        task_id='insert_dummy_data',
        python_callable=insert_orders,
    )

    #extract and load the orders from the table
    etl_extract_load_job = PythonOperator(
        task_id='extract_load_orders',
        python_callable=etl_job,
    )

    # set the task dependencies
    insert_dummy_data >> etl_extract_load_job