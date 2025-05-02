import psycopg2
from psycopg2 import sql
import json

def connect():
    # Load credentials from JSON file
    with open('../etl_config/pg_creds.json') as f:
        creds = json.load(f)

    # Connect to your postgres DB
    conn = psycopg2.connect(
        dbname=creds["DB_NAME"],
        user=creds["DB_USER"],
        password=creds["DB_PASSWORD"],
        host=creds["DB_HOST"],
        port=creds["DB_PORT"],
        # host=creds["DB_HOST_INTERNAL"],
        # port=creds["DB_PORT_INTERNAL"]
    )

    # return the connection
    return conn

