import os
import time
import logging
from google.cloud import pubsub_v1

# Config
logging.basicConfig(level=logging.INFO)
project_id = "pure-karma-387207"
topic_name = "crawl-tasks"
subscription_name = "crawl-sub"

# PubSub Clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(project_id, topic_name)
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def publish_url(url):
    """Publish URL to crawl-tasks topic"""
    data = url.encode("utf-8")
    future = publisher.publish(topic_path, data)
    logging.info(f"Published URL: {url} (Message ID: {future.result()})")

def distribute_tasks(seed_urls):
    """Distribute initial crawling tasks"""
    for url in seed_urls:
        publish_url(url)
        time.sleep(1)  # Respect rate limits

if __name__ == "__main__":
    seed_urls = [
        "https://example.com",
        "https://google.com",
        "https://wikipedia.org"
    ]
    distribute_tasks(seed_urls)