import os
import time
import logging
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("pure-karma-387207", "crawl-tasks")

def publish_initial_urls():
    with open('seeds/initial_urls.txt', 'r') as f:
        for url in f:
            url = url.strip()
            if url:
                publisher.publish(topic_path, url.encode('utf-8'))
                logging.info(f"Published: {url}")
                time.sleep(1)

if __name__ == "__main__":
    publish_initial_urls()