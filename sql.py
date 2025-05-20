import pandas as pd
import mysql.connector
from datetime import datetime

# CSV and DB details
CSV_FILE = "sample_data.csv"
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "product_db"
}
TABLE_NAME = "products"

def clean_discount(discount_str):
    """Remove % and convert to float."""
    try:
        return float(discount_str.replace('%', ''))
    except:
        return 0.0

def parse_date(date_str):
    """Convert dd-mm-yyyy to yyyy-mm-dd"""
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").date()
    except:
        return None

def load_csv_to_mysql():
    df = pd.read_csv(CSV_FILE)

    # Clean columns
    df["Discount"] = df["Discount"].apply(clean_discount)
    df["LaunchDate"] = df["LaunchDate"].apply(parse_date)

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create table
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        ProductID INT PRIMARY KEY,
        ProductName VARCHAR(100),
        Category VARCHAR(100),
        Price FLOAT,
        Rating FLOAT,
        ReviewCount INT,
        Stock INT,
        Discount FLOAT,
        Brand VARCHAR(100),
        LaunchDate DATE
    )
    ''')

    # Insert data
    for _, row in df.iterrows():
        sql = f'''
        INSERT IGNORE INTO {TABLE_NAME} 
        (ProductID, ProductName, Category, Price, Rating, ReviewCount, Stock, Discount, Brand, LaunchDate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = (
            int(row['ProductID']),
            row['ProductName'],
            row['Category'],
            float(row['Price']),
            float(row['Rating']),
            int(row['ReviewCount']),
            int(row['Stock']),
            float(row['Discount']),
            row['Brand'],
            row['LaunchDate']
        )
        cursor.execute(sql, values)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_csv_to_mysql()
