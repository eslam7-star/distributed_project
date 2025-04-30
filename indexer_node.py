import os
import logging
from google.cloud import pubsub_v1
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, ID, TEXT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "pure-karma-387207"
SUBSCRIPTION_NAME = "index-sub"
INDEX_DIR = "index"

schema = Schema(
    url=ID(stored=True),
    title=TEXT(stored=True),
    content=TEXT(stored=True)
)

def initialize_index():
    if not os.path.exists(INDEX_DIR):
        os.makedirs(INDEX_DIR)
        return create_in(INDEX_DIR, schema)
    return open_dir(INDEX_DIR)

def process_message(message):
    try:
        url = message.attributes.get('url', '')
        title = message.attributes.get('title', 'Untitled')
        content = message.data.decode('utf-8') if message.data else ''
        
        if not url:
            logger.warning("Received message without URL")
            return False
            
        with ix.writer() as writer:
            writer.add_document(
                url=url,
                title=title[:255], 
                content=content[:100000]  
            )
        
        logger.info(f"Successfully indexed: {url}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process message: {str(e)}")
        return False

def main():
    global ix
    ix = initialize_index()
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    
    logger.info("Starting indexer service...")
    
    def callback(message):
        if process_message(message):
            message.ack()
        else:
            message.nack()
    
    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
    
    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
        streaming_pull.cancel()
    except Exception as e:
        logger.error(f"Service crashed: {str(e)}")
        raise

if __name__ == "__main__":
    main()