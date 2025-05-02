from etl_load.snowflake_connect import connect
import pandas as pd

def load_orders(df):
    conn = connect()
    cur = conn.cursor()

    # insert the data into the target table
    insert_stmt = """
        insert into orders
        (order_id,customer_id, status, order_date, quantity, unit_price, total_price, year, month, day, hour)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    records = [
        (
            row['order_id'],
            int(row['customer_id']),
            row['status'],
            row['order_date'].strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(row['order_date']) else None,  # Convert datetime to string,
            int(row['quantity']),
            float(row['unit_price']),
            float(row['total_price']),
            int(row['year']),
            int(row['month']),
            int(row['day']),
            int(row['hour']),
        )
        for _,row in df.iterrows()
    ]
    cur.executemany(insert_stmt,records)
    #commit the transaction
    conn.commit()
    print("Commit successfull.")
    # close the cursor and connection
    cur.close()
    conn.close()
    print("Snowflake connection closed.")
    print("--------------------------------------------------")
    print("Data loaded into Snowflake successfully.")
    print("--------------------------------------------------")



