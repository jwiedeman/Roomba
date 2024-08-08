import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import pooling
import random
import time
from concurrent.futures import ThreadPoolExecutor
import curses

# Database configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'domain_scraper'

# Create a connection pool
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=20,
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

# Initialize the database and table
def initialize_db():
    db = connection_pool.get_connection()
    cursor = db.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(DB_NAME))
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS domains (
            id INT AUTO_INCREMENT PRIMARY KEY,
            domain VARCHAR(255) UNIQUE,
            crawled ENUM('yes', 'no') DEFAULT 'no'
        )
    """)
    cursor.execute("INSERT IGNORE INTO domains (domain) VALUES ('wikipedia.org')")
    db.commit()
    cursor.close()
    db.close()

# Get links from a webpage
def get_links_from_page(url, session):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br'
    }
    try:
        response = session.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a.get('href') for a in soup.find_all('a', href=True)]
        return links
    except requests.RequestException:
        return []

# Extract top-level domains from links
def extract_domains(links):
    domains = set()
    for link in links:
        if link.startswith('http'):
            domain = link.split('/')[2]
            domains.add(domain)
    return domains

# Save domains to the database in batches
def save_domains(domains, crawled_status='no'):
    db = connection_pool.get_connection()
    cursor = db.cursor()
    newly_added = []
    insert_query = "INSERT INTO domains (domain, crawled) VALUES (%s, %s) ON DUPLICATE KEY UPDATE domain=domain"
    for domain in domains:
        try:
            cursor.execute(insert_query, (domain, crawled_status))
            newly_added.append(domain)
        except mysql.connector.errors.IntegrityError:
            pass
    db.commit()  # Commit once for all new domains
    cursor.close()
    db.close()
    return newly_added

# Retrieve a single uncrawled domain from the database and mark it as crawled
def get_and_mark_uncrawled_domain():
    db = connection_pool.get_connection()
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("SELECT domain FROM domains WHERE crawled='no' LIMIT 1 FOR UPDATE SKIP LOCKED")
        result = cursor.fetchone()
        if result:
            cursor.execute("UPDATE domains SET crawled='yes' WHERE domain=%s", (result['domain'],))
            db.commit()
            return result['domain']
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        cursor.close()
        db.close()
    return None

# Crawl a domain
def crawl_domain(domain, log, newly_added_log, session):
    log.append(f"Crawling http://{domain}...")
    links = get_links_from_page(f'http://{domain}', session)
    domains = extract_domains(links)
    newly_added = save_domains(domains)
    log.append(f"Finished crawling http://{domain}")
    for new_domain in newly_added:
        newly_added_log.append(f"New domain added: http://{new_domain}")
    time.sleep(0.1)  # Reduce sleep time to increase crawling speed

# Worker function for threads
def worker(log, newly_added_log, last_active_time, thread_id, session):
    while True:
        domain = get_and_mark_uncrawled_domain()
        if domain:
            crawl_domain(domain, log, newly_added_log, session)
        else:
            log.append("No uncrawled domains left.")
            break

        last_active_time[thread_id] = time.time()

# Function to get stats from the database
def get_stats():
    db = connection_pool.get_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(domain) AS total FROM domains")
    total = cursor.fetchone()['total']
    cursor.execute("SELECT COUNT(domain) AS crawled FROM domains WHERE crawled='yes'")
    crawled = cursor.fetchone()['crawled']
    cursor.execute("SELECT COUNT(domain) AS uncrawled FROM domains WHERE crawled='no'")
    uncrawled = cursor.fetchone()['uncrawled']
    cursor.close()
    db.close()
    return total, crawled, uncrawled

# Main function to control the crawling process and update the console
def main(stdscr):
    initialize_db()
    max_threads = 10  # Number of worker threads
    log = []
    newly_added_log = []
    stdscr.nodelay(1)
    curses.start_color()
    curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLACK)  # Default color for crawling logs
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)  # Color for newly added domains

    last_active_time = {i: time.time() for i in range(max_threads)}

    def restart_thread(thread_id):
        log.append(f"Restarting thread {thread_id}")
        last_active_time[thread_id] = time.time()
        session = requests.Session()
        return executor.submit(worker, log, newly_added_log, last_active_time, thread_id, session)

    executor = ThreadPoolExecutor(max_workers=max_threads)
    futures = {i: executor.submit(worker, log, newly_added_log, last_active_time, i, requests.Session()) for i in range(max_threads)}

    while True:
        stdscr.clear()
        total, crawled, uncrawled = get_stats()
        stdscr.addstr(0, 0, f"Total domains: {total:,}")
        stdscr.addstr(1, 0, f"Crawled domains: {crawled:,}")
        stdscr.addstr(2, 0, f"Uncrawled domains: {uncrawled:,}")
        stdscr.addstr(4, 0, "Current Crawling Logs:")
        stdscr.addstr(4, 50, "Newly Added Domains:")

        # Get terminal size
        height, width = stdscr.getmaxyx()

        # Calculate how many logs can fit in the remaining space
        max_log_entries = height - 10

        # Show the last 'max_log_entries' of each log
        for i, log_entry in enumerate(log[-max_log_entries:], start=5):
            stdscr.addstr(i, 0, log_entry[:width-1], curses.color_pair(1))
        for i, log_entry in enumerate(newly_added_log[-max_log_entries:], start=5):
            stdscr.addstr(i, 50, log_entry[:width-51], curses.color_pair(2))

        stdscr.refresh()

        current_time = time.time()
        for thread_id, future in futures.items():
            if future.done() or (current_time - last_active_time[thread_id] > 300):  # Restart if thread is inactive for more than 5 minutes
                futures[thread_id] = restart_thread(thread_id)

        if all(future.done() for future in futures.values()):
            break

        time.sleep(0.1)  # Reduce the refresh rate to 0.1 seconds for faster updates

    executor.shutdown()

if __name__ == "__main__":
    curses.wrapper(main)
