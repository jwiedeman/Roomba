import aiohttp
import asyncio
import mysql.connector.pooling
import logging
import curses
import time
from Wappalyzer import Wappalyzer, WebPage
from multiprocessing import Value
import queue
import threading

# Setup a thread-safe queue for log messages
log_queue = queue.Queue()

# Configure logging to use the queue
class QueueHandler(logging.Handler):
    """ A logging handler that puts logs into a queue """

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(self.format(record))

# Set up logging
queue_handler = QueueHandler(log_queue)
formatter = logging.Formatter('%(asctime)s - %(message)s')
queue_handler.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[queue_handler])

# Database configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'domain_scraper'

# Connection pool configuration
connection_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=25,
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

# Headers to mimic a browser request
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# Initialize the database
def initialize_db():
    db = connection_pool.get_connection()
    cursor = db.cursor()

    # Check if 'domain_status' column exists
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'domains' AND COLUMN_NAME = 'domain_status'
    """)
    if cursor.fetchone() is None:
        cursor.execute("""
            ALTER TABLE domains 
            ADD COLUMN domain_status TEXT DEFAULT NULL
        """)

    # Check if 'cms' column exists
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'domains' AND COLUMN_NAME = 'cms'
    """)
    if cursor.fetchone() is None:
        cursor.execute("""
            ALTER TABLE domains 
            ADD COLUMN cms TEXT DEFAULT NULL
        """)

    db.commit()
    cursor.close()
    db.close()

# Use aiohttp to check if a URL is reachable
async def is_reachable(session, url):
    try:
        async with session.get(url, timeout=5, headers=HEADERS) as response:
            return response.status == 200
    except Exception:
        return False

# Use Wappalyzer to detect technologies and CMS
def analyze_technologies(url):
    try:
        webpage = WebPage.new_from_url(url)
        wappalyzer = Wappalyzer.latest()
        technologies = wappalyzer.analyze_with_versions_and_categories(webpage)
        return technologies
    except Exception:
        return None

async def update_domain_status_and_technologies(domain, reachability_status, technologies):
    db = connection_pool.get_connection()
    cursor = db.cursor()

    try:
        # Create a CSV string with technologies and versions
        if technologies:
            tech_info = []
            for tech, details in technologies.items():
                version_str = ' '.join(details['versions']) if details['versions'] else ''
                tech_info.append(f"{tech} {version_str}".strip())
            tech_info_csv = ', '.join(tech_info)
        else:
            tech_info_csv = 'N/A'

        # Ensure reachability status is a CSV string of all reachable methods
        reachable_methods = [k for k, v in reachability_status.items() if v]
        reachability_text = ', '.join(reachable_methods)
        
        # Update the domains table with the reachability and technology information
        query = """
        INSERT INTO domains (domain, domain_status, cms) 
        VALUES (%s, %s, %s) 
        ON DUPLICATE KEY UPDATE domain_status = %s, cms = %s
        """
        parameters = (domain, reachability_text, tech_info_csv, reachability_text, tech_info_csv)

        cursor.execute(query, parameters)
        db.commit()
    except mysql.connector.Error as err:
        logging.error(f"Error updating domain {domain}: {err}")
    finally:
        cursor.close()
        db.close()

def is_reachable_any(reachability_status):
    return any(v for v in reachability_status.values())

# Check domain reachability and analyze technologies
async def check_domain_status(session, domain, semaphore):
    url_variations = {
        "https": f"https://{domain}",
        "httpswww": f"https://www.{domain}",
        "http": f"http://{domain}",
        "httpwww": f"http://www.{domain}",
    }
    reachability_status = {}
    technologies = None

    async with semaphore:
        for key in ['https', 'httpswww', 'http', 'httpwww']:
            url = url_variations[key]
            is_reachable_flag = await is_reachable(session, url)
            reachability_status[key] = is_reachable_flag

        # Analyze technologies using the first reachable URL
        for key in ['https', 'httpswww', 'http', 'httpwww']:
            if reachability_status[key]:
                url = url_variations[key]
                technologies = analyze_technologies(url)
                break  # Analyze only once using the first reachable URL

    return reachability_status, technologies

# Worker function to process domains
async def process_domain(session, domain, updated_count, total_count, reachable_count, semaphore):
    reachability_status, technologies = await check_domain_status(session, domain, semaphore)

    # Check if any method is reachable
    if is_reachable_any(reachability_status):
        with reachable_count.get_lock():
            reachable_count.value += 1

    await update_domain_status_and_technologies(domain, reachability_status, technologies)
    with updated_count.get_lock():
        updated_count.value += 1
    with total_count.get_lock():
        total_count.value += 1

# Function to get a chunk of domains to be updated
def get_domain_chunk(batch_size=500):
    db = connection_pool.get_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT domain FROM domains WHERE domain_status IS NULL LIMIT %s", (batch_size,))
    domains = cursor.fetchall()
    cursor.close()
    db.close()
    return [domain['domain'] for domain in domains]

# Function to get the total number of domains
def get_total_domains():
    db = connection_pool.get_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(domain) AS total FROM domains")
    total = cursor.fetchone()['total']
    cursor.close()
    db.close()
    return total

# Function to get the number of processed domains
def get_processed_domains():
    db = connection_pool.get_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(domain) AS processed FROM domains WHERE domain_status IS NOT NULL")
    processed = cursor.fetchone()['processed']
    cursor.close()
    db.close()
    return processed

# Function to get the count of domains with detected CMS
def get_cms_detected_count():
    db = connection_pool.get_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(domain) AS cms_count FROM domains WHERE cms IS NOT NULL AND cms != 'N/A'")
    cms_count = cursor.fetchone()['cms_count']
    cursor.close()
    db.close()
    return cms_count

# Function to get the count of reachable domains
def get_reachable_domains_count():
    db = connection_pool.get_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(domain) AS reachable_count FROM domains WHERE domain_status IS NOT NULL AND domain_status != ''")
    reachable_count = cursor.fetchone()['reachable_count']
    cursor.close()
    db.close()
    return reachable_count

# Function to update totals every 2 seconds
def update_totals(stdscr, updated_count, total_domains, reachable_count):
    while True:
        processed_domains = get_processed_domains()
        cms_detected_count = get_cms_detected_count()
        current_reachable_count = get_reachable_domains_count()
        stdscr.addstr(0, 0, f"Total domains: {total_domains:,}")
        stdscr.addstr(1, 0, f"Domains updated: {updated_count.value:,}")
        stdscr.addstr(2, 0, f"Processed domains: {processed_domains:,}")
        stdscr.addstr(3, 0, f"Reachable domains: {current_reachable_count:,}")
        stdscr.addstr(4, 0, f"CMS detected: {cms_detected_count:,}")
        stdscr.addstr(5, 0, "Progress:")

        # Calculate progress percentage
        progress = (processed_domains / total_domains) * 100 if total_domains > 0 else 0
        stdscr.addstr(5, 10, f"{progress:.2f}%", curses.color_pair(2))

        stdscr.refresh()
        time.sleep(10)  # Update totals every 2 seconds

# Main function to check domains in the database
async def check_domains(stdscr):
    initialize_db()
    
    total_domains = get_total_domains()

    # Shared variables for tracking progress
    updated_count = Value('i', 0)
    total_count = Value('i', 0)
    reachable_count = Value('i', 0)

    stdscr.clear()
    stdscr.nodelay(1)
    curses.start_color()
    curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)

    # Start a separate thread to update totals every 2 seconds
    totals_thread = threading.Thread(target=update_totals, args=(stdscr, updated_count, total_domains, reachable_count), daemon=True)
    totals_thread.start()

    # Define a semaphore to limit the number of concurrent tasks
    semaphore = asyncio.Semaphore(100)  # Adjust the number based on your needs and system capacity

    async with aiohttp.ClientSession() as session:
        while True:
            domains = get_domain_chunk()
            if not domains:
                break
            tasks = [process_domain(session, domain, updated_count, total_count, reachable_count, semaphore) for domain in domains]
            await asyncio.gather(*tasks)

    stdscr.clear()
    stdscr.addstr(0, 0, f"Total domains: {total_domains:,}")
    stdscr.addstr(1, 0, f"Domains updated: {updated_count.value:,}")
    stdscr.addstr(2, 0, "All domains processed.")
    stdscr.refresh()
    time.sleep(10)

if __name__ == "__main__":
    curses.wrapper(lambda stdscr: asyncio.run(check_domains(stdscr)))
