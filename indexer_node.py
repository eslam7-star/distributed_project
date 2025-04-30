import os
import logging
from google.cloud import pubsub_v1
from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import Schema, ID, TEXT

logging.basicConfig(level=logging.INFO)
project_id = "pure-karma-387207"
subscription_name = "index-sub"

schema = Schema(
    url=ID(stored=True),
    title=TEXT(stored=True),
    content=TEXT(stored=True)
)

if not os.path.exists("index"):
    os.mkdir("index")
if not exists_in("index"):
    create_in("index", schema)
ix = open_dir("index")

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

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