import mysql.connector
from mysql.connector import pooling
import logging
import re
import unicodedata

# Database configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'domain_scraper'
BATCH_SIZE = 10000  # Number of rows to process in each batch

# Create a connection pool
connection_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=10,
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Normalize domain to avoid duplicates
def normalize_domain(domain):
    # Remove protocol
    domain = re.sub(r'^https?://', '', domain, flags=re.IGNORECASE)
    # Remove "www." prefix
    domain = re.sub(r'^www\.', '', domain, flags=re.IGNORECASE)
    # Remove any fragment identifiers or query parameters
    domain = re.sub(r'#.*$', '', domain)
    domain = re.sub(r'\?.*$', '', domain)
    # Remove trailing slashes
    domain = domain.rstrip('/')
    # Normalize Unicode to avoid inconsistencies
    domain = unicodedata.normalize('NFC', domain)
    # Convert to lowercase
    domain = domain.lower().strip()
    # Remove redundant prefixes
    domain = re.sub(r'^www(\d+)\.', '', domain, flags=re.IGNORECASE)
    # Handle encoded characters
    domain = re.sub(r'%[0-9a-fA-F]{2}', lambda x: chr(int(x.group(0)[1:], 16)), domain)
    # Additional cleaning for unusual subdomains or structures
    domain = re.sub(r'^wwwwww\.|wwwwww\.|^wwww\.|^wwww\.', '', domain)
    return domain

def process_batch(offset):
    db = connection_pool.get_connection()
    cursor = db.cursor(dictionary=True)
    
    try:
        # Fetch all domains for comparison
        cursor.execute("SELECT id, domain FROM domains")
        all_domains = {normalize_domain(row['domain']): row['id'] for row in cursor.fetchall()}

        # Fetch a batch of domains to process
        cursor.execute("SELECT id, domain FROM domains LIMIT %s OFFSET %s", (BATCH_SIZE, offset))
        rows = cursor.fetchall()

        for row in rows:
            original_domain = row['domain']
            domain_id = row['id']
            normalized = normalize_domain(original_domain)

            # Check if the normalized domain already exists in the full set
            if normalized in all_domains and all_domains[normalized] != domain_id:
                # If a duplicate is found, remove the duplicate entry
                logging.info(f"Removing duplicate domain: {original_domain} with id {domain_id}")
                cursor.execute("DELETE FROM domains WHERE id = %s", (domain_id,))
            else:
                # If it's unique in this iteration, update the all_domains map
                all_domains[normalized] = domain_id
                # Update the domain to its normalized form
                if normalized != original_domain:
                    logging.info(f"Updating domain id {domain_id} from {original_domain} to {normalized}")
                    cursor.execute("UPDATE domains SET domain = %s WHERE id = %s", (normalized, domain_id))

        # Commit changes after processing each batch
        db.commit()

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    finally:
        cursor.close()
        db.close()

def dedupe_domains():
    offset = 0

    while True:
        process_batch(offset)
        offset += BATCH_SIZE
        logging.info(f"Processed batch starting at offset {offset}")

        # Check if there are no more rows to process
        db = connection_pool.get_connection()
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT COUNT(*) AS count FROM domains")
        total_domains = cursor.fetchone()['count']
        cursor.close()
        db.close()

        if offset >= total_domains:
            break

if __name__ == "__main__":
    dedupe_domains()
    logging.info("Domain deduplication and normalization complete.")
