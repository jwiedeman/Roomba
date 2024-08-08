import requests
import mysql.connector
import concurrent.futures
import curses
import time
import logging
from multiprocessing import Value

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Database configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'domain_scraper'

# Number of worker threads (Adjust this value to fine-tune performance)
WORKER_THREADS = 10

# Connect to MySQL
def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# Initialize the database to add the domain_status column if it doesn't exist
def initialize_db():
    db = connect_db()
    cursor = db.cursor()

    logging.info("Checking if 'domain_status' column exists in the 'domains' table.")
    # Check if the column already exists
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'domains' AND COLUMN_NAME = 'domain_status'
    """)

    if cursor.fetchone() is None:
        logging.info("Adding 'domain_status' column to the 'domains' table.")
        cursor.execute("""
            ALTER TABLE domains 
            ADD COLUMN domain_status ENUM('http', 'https', 'www', 'non-www', 'unreachable') DEFAULT NULL
        """)
    else:
        logging.info("'domain_status' column already exists.")

    # Reset all domain statuses to NULL at the start
    logging.info("Resetting all domain statuses to NULL.")
    cursor.execute("UPDATE domains SET domain_status = NULL")
    db.commit()

    cursor.close()
    db.close()

# Check if a URL is reachable
def is_reachable(url):
    logging.debug(f"Checking URL: {url}")
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            logging.debug(f"URL reachable: {url}")
            return True
    except requests.RequestException as e:
        logging.debug(f"URL not reachable: {url}, error: {e}")
        return False
    logging.debug(f"URL not reachable: {url}, received non-200 status code.")
    return False

# Check domain reachability using different methods
def check_domain_status(domain):
    methods = ['http://', 'https://', 'http://www.', 'https://www.']
    reachable_methods = []

    logging.info(f"Checking reachability for domain: {domain}")
    for method in methods:
        url = f"{method}{domain}"
        if is_reachable(url):
            reachable_methods.append(method.strip('://'))

    logging.info(f"Reachable methods for {domain}: {reachable_methods}")
    return reachable_methods

# Update the domain status in the database
def update_domain_status(domain, reachable_methods):
    status = ','.join(reachable_methods) if reachable_methods else 'unreachable'
    logging.info(f"Updating domain status for {domain}: {status}")

    db = connect_db()
    cursor = db.cursor()
    cursor.execute("UPDATE domains SET domain_status=%s WHERE domain=%s", (status, domain))
    db.commit()
    cursor.close()
    db.close()

# Worker function to process domains
def process_domain(domain, updated_count, total_count):
    logging.info(f"Processing domain: {domain}")
    reachable_methods = check_domain_status(domain)
    update_domain_status(domain, reachable_methods)
    with updated_count.get_lock():
        updated_count.value += 1
    with total_count.get_lock():
        total_count.value += 1
    logging.info(f"Finished processing domain: {domain}")

# Function to get the list of domains to be updated
def get_domains_to_update():
    logging.info("Retrieving domains to update.")
    db = connect_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT domain FROM domains WHERE domain_status IS NULL")
    domains = cursor.fetchall()
    cursor.close()
    db.close()
    logging.info(f"Retrieved {len(domains)} domains to update.")
    return [domain['domain'] for domain in domains]

# Function to get the total number of domains
def get_total_domains():
    logging.info("Retrieving total number of domains.")
    db = connect_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(domain) AS total FROM domains")
    total = cursor.fetchone()['total']
    cursor.close()
    db.close()
    logging.info(f"Total domains in the database: {total}")
    return total

# Function to get the number of processed domains
def get_processed_domains():
    logging.info("Retrieving number of processed domains.")
    db = connect_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(domain) AS processed FROM domains WHERE domain_status IS NOT NULL")
    processed = cursor.fetchone()['processed']
    cursor.close()
    db.close()
    logging.info(f"Processed domains in the database: {processed}")
    return processed

# Main function to check domains in the database
def check_domains(stdscr):
    logging.info("Initializing database and resetting domain statuses.")
    initialize_db()
    
    logging.info("Fetching domains to update.")
    domains = get_domains_to_update()

    logging.info("Fetching total number of domains.")
    total_domains = get_total_domains()

    num_domains = len(domains)
    logging.info(f"Total domains to process: {num_domains}")

    # Shared variables for tracking progress
    updated_count = Value('i', 0)
    total_count = Value('i', 0)

    stdscr.clear()
    stdscr.nodelay(1)
    curses.start_color()
    curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLACK)  # Default color for status
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)  # Color for completed tasks

    logging.info("Starting domain reachability check.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        futures = [executor.submit(process_domain, domain, updated_count, total_count) for domain in domains]

        while any(not f.done() for f in futures):
            stdscr.clear()
            processed_domains = get_processed_domains()
            stdscr.addstr(0, 0, f"Total domains: {total_domains:,}")
            stdscr.addstr(1, 0, f"Domains to update: {num_domains:,}")
            stdscr.addstr(2, 0, f"Domains updated: {updated_count.value:,}")
            stdscr.addstr(3, 0, f"Processed domains: {processed_domains:,}")
            stdscr.addstr(5, 0, "Progress:")

            # Calculate progress percentage
            progress = (updated_count.value / num_domains) * 100 if num_domains > 0 else 0
            stdscr.addstr(5, 10, f"{progress:.2f}%", curses.color_pair(2))

            # Refresh the console
            stdscr.refresh()
            time.sleep(0.5)  # Refresh interval to give time for updates

        # Final update after all are done
        stdscr.clear()
        stdscr.addstr(0, 0, f"Total domains: {total_domains:,}")
        stdscr.addstr(1, 0, f"Domains updated: {updated_count.value:,}")
        stdscr.addstr(2, 0, "All domains processed.")
        stdscr.refresh()
        logging.info("All domains processed.")
        time.sleep(2)

if __name__ == "__main__":
    logging.info("Starting the domain status updater script.")
    curses.wrapper(check_domains)
