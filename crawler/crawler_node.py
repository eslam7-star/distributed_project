import os
import time
import logging
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup
import requests

logging.basicConfig(level=logging.INFO)
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path("pure-karma-387207", "crawl-sub")

def process_page(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        return {
            'url': url,
            'title': soup.title.string if soup.title else '',
            'content': soup.get_text()
        }
    except Exception as e:
        logging.error(f"Failed to crawl {url}: {e}")
        return None

def callback(message):
    try:
        url = message.data.decode('utf-8')
        logging.info(f"Processing: {url}")
        
        data = process_page(url)
        if data:
            # أرسل للفهرسة
            publisher = pubsub_v1.PublisherClient()
            index_topic = publisher.topic_path("your-project-id", "index-tasks")
            publisher.publish(index_topic, str(data).encode('utf-8'))
        
        message.ack()
    except Exception as e:
        logging.error(f"Message processing failed: {e}")
        message.nack()

if __name__ == "__main__":
    subscriber.subscribe(subscription_path, callback=callback)
    logging.info("Crawler started. Waiting for messages...")
    while True:
        time.sleep(60)