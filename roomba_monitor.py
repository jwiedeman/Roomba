import mysql.connector
from mysql.connector import pooling
import curses
import threading
import time
import logging

# Database configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'domain_scraper'

# Create a connection pool
connection_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to periodically update stats from the database
def update_stats(stats, lock):
    while True:
        db = connection_pool.get_connection()
        cursor = db.cursor(dictionary=True)
        
        try:
            cursor.execute("SELECT COUNT(domain) AS total FROM domains")
            total = cursor.fetchone()['total']
            cursor.execute("SELECT COUNT(domain) AS crawled FROM domains WHERE crawled='yes'")
            crawled = cursor.fetchone()['crawled']
            cursor.execute("SELECT COUNT(domain) AS uncrawled FROM domains WHERE crawled='no'")
            uncrawled = cursor.fetchone()['uncrawled']
        except mysql.connector.Error as err:
            logging.error(f"Database error: {err}")
        finally:
            cursor.close()
            db.close()

        # Update shared stats with lock
        with lock:
            stats['total'] = total
            stats['crawled'] = crawled
            stats['uncrawled'] = uncrawled

        time.sleep(1)  # Update stats every second

# Main function to update the console display
def main(stdscr):
    stats = {'total': 0, 'crawled': 0, 'uncrawled': 0}
    lock = threading.Lock()

    # Start a separate thread for updating stats
    stats_thread = threading.Thread(target=update_stats, args=(stats, lock))
    stats_thread.daemon = True
    stats_thread.start()

    stdscr.nodelay(1)
    curses.start_color()

    while True:
        stdscr.clear()

        # Safely read stats with lock
        with lock:
            total = stats['total']
            crawled = stats['crawled']
            uncrawled = stats['uncrawled']

        stdscr.addstr(0, 0, f"Total domains: {total:,}")
        stdscr.addstr(1, 0, f"Crawled domains: {crawled:,}")
        stdscr.addstr(2, 0, f"Uncrawled domains: {uncrawled:,}")

        stdscr.refresh()

        time.sleep(1)  # Refresh console every second

if __name__ == "__main__":
    curses.wrapper(main)
