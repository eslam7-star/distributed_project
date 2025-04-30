import os
import time
import logging
from google.cloud import pubsub_v1

# Config
logging.basicConfig(level=logging.INFO)
project_id = "pure-karma-387207"
topic_crawl = "crawl-tasks"
topic_index = "index-tasks"

# PubSub Clients
publisher = pubsub_v1.PublisherClient()
crawl_topic = publisher.topic_path(project_id, topic_crawl)
index_topic = publisher.topic_path(project_id, topic_index)

def publish_task(topic, data):
    try:
        future = publisher.publish(topic, data.encode('utf-8'))
        return future.result()
    except Exception as e:
        logging.error(f"Publish failed: {e}")
        return None

def distribute_tasks():
    seed_urls = [
        "https://example.com",
        "https://google.com",
        "https://wikipedia.org"
    ]
   
    for url in seed_urls:
        task_id = publish_task(crawl_topic, url)
        if task_id:
            logging.info(f"Published crawl task: {url} (ID: {task_id})")
        time.sleep(1)  # Politeness delay

if __name__ == "__main__":
    logging.info("Master node started")
    distribute_tasks()

