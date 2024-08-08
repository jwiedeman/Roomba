import mysql.connector
import pandas as pd

# Database configuration
DB_HOST = 'localhost'
DB_USER = 'root '
DB_PASSWORD = '1234'
DB_NAME = 'domain_scraper'
CSV_FILE = 'domains_export.csv'

# Connect to MySQL
def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# Export domains table to CSV
def export_to_csv():
    db = connect_db()
    query = "SELECT * FROM domains"
    df = pd.read_sql(query, db)
    df.to_csv(CSV_FILE, index=False)
    db.close()
    print(f"Exported data to {CSV_FILE}")

if __name__ == "__main__":
    export_to_csv()
