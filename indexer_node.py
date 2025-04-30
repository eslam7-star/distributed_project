import os
import logging
from whoosh.index import create_in
from whoosh.fields import *
from google.cloud import pubsub_v1

# Config
logging.basicConfig(level=logging.INFO)
project_id = "pure-karma-387207"
subscription_name = "index-tasks"

# Whoosh Index Setup
schema = Schema(
    url=ID(stored=True),
    title=TEXT(stored=True),
    content=TEXT
)
if not os.path.exists("index"):
    os.mkdir("index")
ix = create_in("index", schema)

# PubSub Client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def index_page(url, title, content):
    """Index webpage content"""
    writer = ix.writer()
    writer.add_document(
        url=url,
        title=title,
        content=content
    )
    writer.commit()
    logging.info(f"Indexed: {url}")

def callback(message):
    """Process indexing tasks"""
    data = message.data.decode('utf-8').split('|')
    url, title, content = data[0], data[1], data[2]
    
    index_page(url, title, content)
    message.ack()

if __name__ == "__main__":
    logging.info("Indexer node started. Waiting for tasks...")
    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
    
    try:
        streaming_pull.result()
    except Exception as e:
        logging.error(f"Subscription error: {str(e)}")
        streaming_pull.cancel()