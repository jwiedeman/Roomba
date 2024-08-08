import requests
from bs4 import BeautifulSoup
import mysql.connector
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import curses
from mysql.connector import pooling

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
    insert_query = "INSERT INTO domains (domain, crawled) VALUES (%s, %s)"
    for domain in domains:
        try:
            cursor.execute(insert_query, (domain, crawled_status))
            db.commit()
            newly_added.append(domain)
        except mysql.connector.errors.IntegrityError:
            pass
    cursor.close()
    db.close()
    return newly_added

# Read uncrawled domain from the database
def get_uncrawled_domain():
    db = connection_pool.get_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT domain FROM domains WHERE crawled='no' ORDER BY RAND() LIMIT 1")
    result = cursor.fetchone()
    cursor.close()
    db.close()
    return result['domain'] if result else None

# Mark a domain as crawled
def mark_domain_as_crawled(domain):
    db = connection_pool.get_connection()
    cursor = db.cursor()
    cursor.execute("UPDATE domains SET crawled='yes' WHERE domain=%s", (domain,))
    db.commit()
    cursor.close()
    db.close()

# Crawl a domain
def crawl_domain(domain, log, newly_added_log, session):
    log.append(f"Crawling http://{domain}...")
    links = get_links_from_page(f'http://{domain}', session)
    domains = extract_domains(links)
    newly_added = save_domains(domains)
    mark_domain_as_crawled(domain)
    log.append(f"Finished crawling http://{domain}")
    for new_domain in newly_added:
        newly_added_log.append(f"New domain added: http://{new_domain}")
    time.sleep(0.5)  # Slow down the crawling to avoid hammering servers

# Recrawl a domain for deeper links
def recrawl_domain(domain, log, newly_added_log, session):
    log.append(f"Recrawling http://{domain} for deeper links...")
    sitemap_url = f'http://{domain}/sitemap.xml'
    links = get_links_from_page(sitemap_url, session)
    additional_links = links[:10]  # Taking the first 10 links
    for link in additional_links:
        links += get_links_from_page(link, session)
    domains = extract_domains(links)
    newly_added = save_domains(domains)
    log.append(f"Finished recrawling http://{domain}")
    for new_domain in newly_added:
        newly_added_log.append(f"New domain added: http://{new_domain}")
    time.sleep(0.5)  # Slow down the crawling to avoid hammering servers

# Worker function for threads
def worker(log, newly_added_log, last_active_time, thread_id, session):
    while True:
        domain = get_uncrawled_domain()
        if domain:
            crawl_domain(domain, log, newly_added_log, session)
        else:
            db = connection_pool.get_connection()
            cursor = db.cursor(dictionary=True)
            cursor.execute("SELECT domain FROM domains WHERE crawled='yes'")
            results = cursor.fetchall()
            cursor.close()
            db.close()
            if results:
                random_domain = random.choice(results)['domain']
                recrawl_domain(random_domain, log, newly_added_log, session)
            else:
                log.append("No domains left to crawl.")
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
            stdscr.addstr(i, 0, log_entry, curses.color_pair(1))
        for i, log_entry in enumerate(newly_added_log[-max_log_entries:], start=5):
            stdscr.addstr(i, 50, log_entry, curses.color_pair(2))

        stdscr.refresh()

        current_time = time.time()
        for thread_id, future in futures.items():
            if future.done() or (current_time - last_active_time[thread_id] > 300):  # Restart if thread is inactive for more than 5 minutes
                futures[thread_id] = restart_thread(thread_id)

        if all(future.done() for future in futures.values()):
            break

        time.sleep(1)  # Refresh the console every second

    executor.shutdown()

if __name__ == "__main__":
    curses.wrapper(main)
