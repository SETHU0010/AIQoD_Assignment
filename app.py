import pandas as pd
import mysql.connector
from datetime import datetime

CSV_FILE = "sample_data.csv"
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "product_db"
}
TABLE_NAME = "products"

def load_csv_to_mysql():
    # Load the CSV
    df = pd.read_csv(CSV_FILE)
    
    # Strip spaces from column names
    df.columns = df.columns.str.strip()

    # Debug print to check column names
    print("Detected CSV Columns:", df.columns.tolist())

    # Required columns
    required_cols = ['ProductID', 'Name', 'Category', 'Rating', 'Reviews',
                     'Brand', 'Stock', 'LaunchDate', 'Discount', 'Price']
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        print(f"Missing columns in CSV: {missing_cols}")
        return

    # Connect to MySQL and create table if not exists
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        ProductID INT PRIMARY KEY,
        Name VARCHAR(100),
        Category VARCHAR(100),
        Rating FLOAT,
        Reviews INT,
        Brand VARCHAR(50),
        Stock INT,
        LaunchDate DATE,
        Discount FLOAT,
        Price FLOAT
    )
    ''')

    # Insert rows
    for _, row in df.iterrows():
        try:
            sql = f'''
            INSERT IGNORE INTO {TABLE_NAME} 
            (ProductID, Name, Category, Rating, Reviews, Brand, Stock, LaunchDate, Discount, Price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''

            launch_date = row['LaunchDate']
            if isinstance(launch_date, str):
                try:
                    launch_date = datetime.strptime(launch_date, "%Y-%m-%d").date()
                except:
                    launch_date = None

            cursor.execute(sql, (
                int(row['ProductID']),
                row['Name'],
                row['Category'],
                float(row['Rating']),
                int(row['Reviews']),
                row['Brand'],
                int(row['Stock']),
                launch_date,
                float(row['Discount']),
                float(row['Price'])
            ))
        except Exception as e:
            print(f"Skipping row due to error: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data successfully loaded into MySQL!")

def get_sql_queries():
    return {
        1: """SELECT * FROM products 
              WHERE Rating < 4.5 AND Reviews > 200 
              AND Brand IN ('Nike', 'Sony')""",

        2: """SELECT * FROM products 
              WHERE Category = 'Electronics' 
              AND Rating >= 4.5 
              AND Stock > 0""",

        3: """SELECT * FROM products 
              WHERE LaunchDate > '2022-01-01' 
              AND Category IN ('Home & Kitchen', 'Sports') 
              AND Discount >= 10 
              ORDER BY Price DESC"""
    }

def run_queries_and_save():
    queries = get_sql_queries()
    conn = mysql.connector.connect(**DB_CONFIG)

    for test_case, query in queries.items():
        df = pd.read_sql(query, conn)
        df.to_csv(f"test_case{test_case}.csv", index=False)

    conn.close()

    with open("Queries_generated.txt", "w") as f:
        for test_case, query in queries.items():
            f.write(f"Test Case {test_case}:\n{query}\n\n")
    print("✅ SQL queries executed and output saved.")

def main():
    load_csv_to_mysql()
    run_queries_and_save()

if __name__ == "__main__":
    main()
