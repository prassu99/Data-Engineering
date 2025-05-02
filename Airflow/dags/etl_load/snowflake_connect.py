import snowflake.connector
import json

def connect():
    with open('/opt/airflow/dags/etl_config/snow_creds.json') as f:
        creds = json.load(f)
        conn = snowflake.connector.connect(
            user=creds["snow_user"],
            password=creds["snow_password"],
            account=creds["snow_account"],
            warehouse=creds["snow_warehouse"],
            database=creds["snow_database"],
            schema=creds["snow_schema"]
    )

    # return the connection
    return conn