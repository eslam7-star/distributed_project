import os
import logging
import re
from google.cloud import pubsub_v1
from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import Schema, ID, TEXT
from whoosh.analysis import StemmingAnalyzer, StandardAnalyzer

logging.basicConfig(level=logging.INFO)
project_id = "pure-karma-387207"
subscription_name = "index-sub"

schema = Schema(
    url=ID(stored=True),
    title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
    content=TEXT(stored=True, analyzer=StandardAnalyzer())
)

if not os.path.exists("index"):
    os.makedirs("index")
if not exists_in("index"):
    create_in("index", schema)
ix = open_dir("index")

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def preprocess_text(text):
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def index_document(url, title, content):
    writer = ix.writer()
    writer.add_document(
        url=url,
        title=title,
        content=preprocess_text(content)
    )
    writer.commit()
    logging.info(f"Indexed: {title[:50]}...")

def callback(message):
    try:
        url = message.attributes.get('url', '')
        title = message.attributes.get('title', 'No Title')
        content = message.data.decode('utf-8')
        index_document(url, title, content)
        message.ack()
    except Exception as e:
        logging.error(f"Indexing error: {e}")
        message.nack()

if __name__ == "__main__":
    logging.info("Indexer service started")
    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull.result()
    except Exception as e:
        logging.error(f"Service stopped: {e}")
        streaming_pull.cancel()