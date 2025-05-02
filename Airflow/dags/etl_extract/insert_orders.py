from dotenv import load_dotenv
from etl_extract.connect import connect
import random
from psycopg2 import sql
from datetime import datetime, timedelta

def insert_orders():
    # connect to the database 
    conn = connect()
    cur=conn.cursor()

    # insert 100 randon records into the table
    for i in range(100):
        an_hour_ago = datetime.now() - timedelta(hours=2)
        cur.execute(
            sql.SQL("INSERT INTO orders (customer_id, status, quantity, order_date, unit_price) VALUES (%s, %s, %s, %s, %s)"),
            (
                random.randint(1, 10),
                'Completed' if random.randint(0, 1) == 0 else 'pending' if random.randint(0, 1) == 0 else 'Cancelled',
                random.randint(1, 10),
                #'2025-'+str(random.randint(1, 12))+'-'+str(random.randint(1, 28)),
                an_hour_ago,
                random.randint(1, 100)
            )
        )

    # commit the changes
    conn.commit()
    print("Inserted 100 random records into the orders table ");
    cur.close()
    conn.close()

