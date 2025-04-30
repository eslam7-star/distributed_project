import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from google.cloud import pubsub_v1

# Config
logging.basicConfig(level=logging.DEBUG)
project_id = "pure-karma-387207"
topic_crawl = "crawl-tasks"
topic_index = "index-tasks"

# PubSub Clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
crawl_subscription = subscriber.subscription_path(project_id, "crawl-sub")

def check_robots(url):
    rp = RobotFileParser()
    try:
        rp.set_url(f"{url}/robots.txt")
        rp.read()
        return rp.can_fetch("*", url)
    except:
        return True  # Default allow if robots.txt unreachable

def extract_links(url, html):
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.startswith('http'):
            links.append(href)
    return links

def process_page(url):
    try:
        if not check_robots(url):
            logging.warning(f"Skipping {url} (disallowed by robots.txt)")
            return None, None, None

        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string if soup.title else "No Title"
        content = soup.get_text(separator=' ', strip=True)
        links = extract_links(url, response.text)
        
        return title, content, links
    
    except Exception as e:
        logging.error(f"Error processing {url}: {e}")
        return None, None, None

def callback(message):
    url = message.data.decode('utf-8')
    logging.info(f"Processing: {url}")
    
    title, content, links = process_page(url)
    
    if title and content:
        # Send to indexer
        index_data = f"{url}|{title}|{content}"
        publisher.publish(index_topic, index_data.encode('utf-8'))
    
    if links:
        for link in links:
            publisher.publish(crawl_topic, link.encode('utf-8'))
    
    message.ack()
    time.sleep(2)  # Crawl delay

if __name__ == "__main__":
    logging.info("Worker node started")
    streaming_pull = subscriber.subscribe(crawl_subscription, callback=callback)
    
    try:
        streaming_pull.result()
    except Exception as e:
        logging.error(f"Worker error: {e}")
        streaming_pull.cancel()