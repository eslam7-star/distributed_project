import json
import logging
from google.cloud import pubsub_v1
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)
es = Elasticsearch(['http://elasticsearch:9200'])
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path("pure-karma-387207", "index-sub")

def index_document(data):
    try:
        es.index(
            index='web_pages',
            body={
                'url': data['url'],
                'title': data['title'],
                'content': data['content'],
                'timestamp': 'now'
            }
        )
        return True
    except Exception as e:
        logging.error(f"Indexing failed: {e}")
        return False

def callback(message):
    try:
        data = json.loads(message.data.decode('utf-8'))
        if index_document(data):
            message.ack()
        else:
            message.nack()
    except Exception as e:
        logging.error(f"Message processing failed: {e}")
        message.nack()

if __name__ == "__main__":
    subscriber.subscribe(subscription_path, callback=callback)
    logging.info("Indexer started. Waiting for messages...")
    while True:
        time.sleep(60)