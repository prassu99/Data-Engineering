from psycopg2 import sql
from Airflow.dags.etl_extract.connect import connect
import pandas   as pd

def extract_orders():
    # connect to the database 
    conn = connect()
    cur=conn.cursor()

    # extract the orders from the table
    print("Extracting orders from the database...")
    cur.execute(
        sql.SQL("SELECT * FROM orders WHERE order_date <= NOW() - INTERVAL '1 hour'")
    )
    rows = cur.fetchall()

    # print the rows
    print("Extracted orders:")
    print("--------------------------------------------------")
    print("customer_id | status | quantity | order_date | unit_price")
    print("--------------------------------------------------")
    # iterate over the rows and print them 
    for row in rows:
        print(row)
    
    # convert the rows to a pandas DataFrame
    df = pd.DataFrame(rows, columns=['order_id', 'customer_id', 'status', 'order_date', 'unit_price','quantity'])
    # print the DataFrame

    # close the cursor and connection
    cur.close()
    conn.close()
    return df