import requests
import zipfile
import io
import mysql.connector
from bs4 import BeautifulSoup
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'domain_scraper'

# Connect to MySQL
def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# Initialize the database and table
def initialize_db():
    db = connect_db()
    cursor = db.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(DB_NAME))
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS domains (
            id INT AUTO_INCREMENT PRIMARY KEY,
            domain VARCHAR(255) UNIQUE,
            crawled ENUM('yes', 'no') DEFAULT 'no'
        )
    """)
    db.commit()
    cursor.close()
    db.close()

# Function to download and extract domain lists
def download_and_extract(url):
    try:
        logging.info(f"Attempting to download: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Handle application/octet-stream content type as a ZIP file
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for file_info in z.infolist():
                if file_info.filename.endswith('.txt'):
                    with z.open(file_info) as f:
                        domains = [line.strip() for line in f.read().decode('utf-8').splitlines()]
                        return domains
    except requests.HTTPError as e:
        logging.error(f"Failed to download {url}: {e}")
    except requests.Timeout:
        logging.error(f"Timeout occurred for {url}")
    except zipfile.BadZipFile:
        logging.error(f"Invalid zip file from {url}")
    return []

# Save domains to the database
def save_domains(domains):
    db = connect_db()
    cursor = db.cursor()
    newly_added = []
    for domain in domains:
        try:
            cursor.execute("INSERT INTO domains (domain, crawled) VALUES (%s, 'no')", (domain,))
            db.commit()
            newly_added.append(domain)
        except mysql.connector.errors.IntegrityError:
            pass  # Ignore duplicates
    cursor.close()
    db.close()
    return newly_added

# Main function to scrape and download NRD lists
def scrape_and_download_nrd_lists():
    initialize_db()
    url = 'https://www.whoisds.com/newly-registered-domains'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all download links for NRD zip files
    download_links = soup.find_all('a', href=True)
    for link in download_links:
        href = link['href']
        if '/whois-database/newly-registered-domains/' in href and href.endswith('/nrd'):
            # Construct the full URL correctly
            if href.startswith('http'):
                full_url = href
            else:
                full_url = 'https://www.whoisds.com' + href
            logging.info(f"Downloading {full_url}")
            domains = download_and_extract(full_url)
            if domains:
                newly_added = save_domains(domains)
                logging.info(f"Added {len(newly_added)} new domains from {full_url}")
            # Add a delay to prevent server overload
            time.sleep(1)

if __name__ == "__main__":
    scrape_and_download_nrd_lists()
