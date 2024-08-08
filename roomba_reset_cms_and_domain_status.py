import mysql.connector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Database configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'domain_scraper'

def connect_db():
    """Connect to the MySQL database."""
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def reset_cms_and_domain_status():
    """Reset all cms and domain_status columns to NULL in the domains table."""
    db = connect_db()
    cursor = db.cursor()

    logging.info("Setting all cms and domain_status columns to NULL.")

    # Reset cms and domain_status to NULL
    cursor.execute("UPDATE domains SET cms = NULL, domain_status = NULL")
    db.commit()

    cursor.close()
    db.close()
    logging.info("All cms and domain_status columns have been reset to NULL.")

if __name__ == "__main__":
    reset_cms_and_domain_status()
