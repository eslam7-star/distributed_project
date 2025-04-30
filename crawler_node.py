import os
import time
import logging
import requests
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1

# Config
logging.basicConfig(level=logging.INFO)
project_id = "pure-karma-387207"
topic_name = "crawl-tasks"
subscription_name = "crawl-sub"

# PubSub Client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def process_url(url):
    """Fetch and parse webpage"""
    try:
        # Respect robots.txt and add delay
        time.sleep(2)
        
        # Fetch page
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Parse content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract data
        title = soup.title.string if soup.title else "No Title"
        links = [a['href'] for a in soup.find_all('href') if a.get('href')]
        
        logging.info(f"Crawled: {url} | Title: {title} | Links: {len(links)}")
        return links
        
    except Exception as e:
        logging.error(f"Failed to crawl {url}: {str(e)}")
        return []

def callback(message):
    """Process incoming Pub/Sub messages"""
    url = message.data.decode('utf-8')
    logging.info(f"Received URL: {url}")
    
    links = process_url(url)
    message.ack()
    
    # Publish new links (optional)
    if links:
        publisher = pubsub_v1.PublisherClient()
        for link in links:
            publisher.publish(topic_path, link.encode('utf-8'))

if __name__ == "__main__":
    logging.info("Worker node started. Waiting for tasks...")
    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
    
    try:
        streaming_pull.result()
    except Exception as e:
        logging.error(f"Subscription error: {str(e)}")
        streaming_pull.cancel()