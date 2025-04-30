import os
import logging
from whoosh.index import create_in
from whoosh.fields import *
from google.cloud import pubsub_v1

# Config
logging.basicConfig(level=logging.DEBUG)
project_id = "pure-karma-387207"
index_subscription = "index-sub"

# Whoosh Index
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
subscription_path = subscriber.subscription_path(project_id, index_subscription)

def index_document(url, title, content):
    writer = ix.writer()
    writer.add_document(
        url=url,
        title=title,
        content=content
    )
    writer.commit()
    logging.info(f"Indexed: {title[:50]}...")

def callback(message):
    try:
        data = message.data.decode('utf-8').split('|')
        if len(data) == 3:
            url, title, content = data
            index_document(url, title, content)
        else:
            logging.error("Invalid message format")
        message.ack()
    except Exception as e:
        logging.error(f"Indexing failed: {e}")
        message.nack()

if __name__ == "__main__":
    logging.info("Indexer node started")
    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
    
    try:
        streaming_pull.result()
    except Exception as e:
        logging.error(f"Indexer error: {e}")
        streaming_pull.cancel()