import pandas as pd

def transform_orders(df):
    print("Transforming orders...")
    print(df.shape)
    """
    Transform the orders DataFrame by converting the 'order_date' column to datetime format
    and extracting the year and month into separate columns.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the orders data.
        
    Returns:
        pd.DataFrame: The transformed DataFrame with 'order_date', 'year', and 'month' columns.
    """
    # Convert 'order_date' to datetime format
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    # Extract year and month from 'order_date'
    df['year'] = df['order_date'].dt.year
    df['month'] = df['order_date'].dt.month
    df['day'] = df['order_date'].dt.day
    df['hour'] = df['order_date'].dt.hour


    # calculate the total price
    df['total_price'] = df['quantity'] * df['unit_price']

    print("Transformed orders:")
    print("--------------------------------------------------")
    print("customer_id | status | quantity | order_date | Quantity | unit_price | total_price | year | month | day | hour")
    print("--------------------------------------------------") 
    # iterate over the rows and print them 
    for index, row in df.iterrows():
        print(f"{row['customer_id']} | {row['status']} | {row['order_date']}| {row['quantity']} | {row['unit_price']} | {row['total_price']} | {row['year']} | {row['month']} | {row['day']} | {row['hour']} ")
        
    print("--------------------------------------------------")
    return df