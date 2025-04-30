#!/usr/bin/env python3
import os
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
from google.cloud import pubsub_v1

# Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pub/Sub Settings
PROJECT_ID = "pure-karma-387207"
TOPIC_CRAWL = "crawl-tasks"
TOPIC_INDEX = "index-tasks"
SUBSCRIPTION_NAME = "crawl-sub"

# Initialize Pub/Sub clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

# Topic paths
crawl_topic = publisher.topic_path(PROJECT_ID, TOPIC_CRAWL)
index_topic = publisher.topic_path(PROJECT_ID, TOPIC_INDEX)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

# Crawler settings
USER_AGENT = "Mozilla/5.0 (compatible; DistributedWebCrawler/1.0)"
REQUEST_TIMEOUT = 10
CRAWL_DELAY = 2  # seconds between requests

class WebCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})

    def check_robots_permission(self, url):
        """Check robots.txt permissions"""
        try:
            domain = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            rp = RobotFileParser()
            rp.set_url(f"{domain}/robots.txt")
            rp.read()
            return rp.can_fetch(USER_AGENT, url)
        except Exception as e:
            logger.warning(f"Robots.txt check failed for {url}: {e}")
            return True

    def fetch_page(self, url):
        """Fetch and parse webpage content"""
        try:
            if not self.check_robots_permission(url):
                logger.warning(f"Skipping {url} (disallowed by robots.txt)")
                return None, None, []

            response = self.session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.title.string if soup.title else "No Title"
            content = soup.get_text(separator=' ', strip=True)
            
            # Extract valid absolute URLs
            links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('http'):
                    links.append(href)
                elif href.startswith('/'):
                    links.append(f"{urlparse(url).scheme}://{urlparse(url).netloc}{href}")

            return title, content, links

        except Exception as e:
            logger.error(f"Failed to process {url}: {e}")
            return None, None, []

    def publish_to_pubsub(self, topic, data, **attributes):
        """Publish data to Pub/Sub topic"""
        try:
            future = publisher.publish(
                topic,
                data.encode('utf-8'),
                **attributes
            )
            message_id = future.result()
            logger.debug(f"Published to {topic}: {message_id}")
            return True
        except Exception as e:
            logger.error(f"Pub/Sub publish failed: {e}")
            return False

def process_message(message, crawler):
    """Process incoming Pub/Sub message"""
    try:
        url = message.data.decode('utf-8')
        logger.info(f"Processing URL: {url}")

        title, content, links = crawler.fetch_page(url)
        
        if title and content:
            # Send to indexer
            crawler.publish_to_pubsub(
                index_topic,
                content,
                url=url,
                title=title[:200]  # Truncate long titles
            )

        # Schedule new crawl tasks
        for link in links[:10]:  # Limit to 10 links per page
            crawler.publish_to_pubsub(crawl_topic, link)

        message.ack()
        time.sleep(CRAWL_DELAY)

    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        message.nack()

def main():
    logger.info("Starting crawler service...")
    crawler = WebCrawler()

    def callback(message):
        process_message(message, crawler)

    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        logger.info("Crawler stopped by user")
        streaming_pull.cancel()
    except Exception as e:
        logger.error(f"Crawler crashed: {e}")
        raise

if __name__ == "__main__":
    main()