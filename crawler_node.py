from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

# Simple politeness delay in seconds
CRAWL_DELAY = 2

def extract_links_and_text(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    
    links = []
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        absolute_url = urljoin(base_url, href)
        if urlparse(absolute_url).scheme in ["http", "https"]:
            links.append(absolute_url)
    
    return text, links

def crawler_process():
    """
    Process for a crawler node.
    Fetches web pages, extracts text and URLs, and sends results back to the master.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logging.info(f"Crawler node started with rank {rank} of {size}")

    while True:
        status = MPI.Status()
        url_to_crawl = comm.recv(source=0, tag=0, status=status)  # Receive task from master

        if not url_to_crawl:  # Optional shutdown signal
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url_to_crawl}")
        try:
            response = requests.get(url_to_crawl, timeout=10)
            response.raise_for_status()

            page_text, extracted_urls = extract_links_and_text(response.text, url_to_crawl)

            logging.info(f"Crawler {rank} crawled {url_to_crawl}, found {len(extracted_urls)} links.")

            result = {
                "url": url_to_crawl,
                "text": page_text,
                "urls": extracted_urls
            }

            comm.send(result, dest=0, tag=1)  # Send data to master (tag 1 = crawl result)
            comm.send(f"Crawler {rank} completed: {url_to_crawl}", dest=0, tag=99)  # Heartbeat/status

        except Exception as e:
            logging.error(f"Crawler {rank} error crawling {url_to_crawl}: {e}")
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)  # Error to master

        time.sleep(CRAWL_DELAY)  # Politeness delay

if __name__ == '__main__':
    crawler_process()