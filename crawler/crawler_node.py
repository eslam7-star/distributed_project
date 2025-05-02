import time
import json
import redis
import requests
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1
from urllib.parse import urljoin
import os

PROJECT_ID = "pure-karma-387207"
TOPIC_NAME = "crawl-tasks"
SUBSCRIPTION_NAME = "crawl-sub"
INDEX_TOPIC_NAME = "index-tasks"
TIMEOUT = 60

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

redis_host = os.getenv("REDIS_HOST", "localhost")
redis_client = redis.Redis(host=redis_host, port=6379, db=0)
heartbeat_key = "task_heartbeat"
status_key = "task_status"
url_key = "task_urls"

def fetch_and_process_page(url, task_id):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, "html.parser")
            links = extract_links(soup, url)
            send_to_indexer(task_id, url, html_content)
            update_heartbeat(task_id)
            return links
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
    return []

def extract_links(soup, base_url):
    links = set()
    for a_tag in soup.find_all("a", href=True):
        link = a_tag.get("href")
        full_link = urljoin(base_url, link)
        links.add(full_link)
    return list(links)

def send_to_indexer(task_id, url, html_content):
    message = {
        "task_id": task_id,
        "url": url,
        "html_content": html_content
    }
    publisher.publish(topic_path, json.dumps(message).encode("utf-8"))

def update_heartbeat(task_id):
    redis_client.hset(heartbeat_key, task_id, int(time.time()))

def process_task(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        task_id = str(data["task_id"])
        url = data["url"]
        print(f"[FETCH] Crawling {url} for task {task_id}")
        links = fetch_and_process_page(url, task_id)
        for link in links:
            add_task(link)
        message.ack()
    except Exception as e:
        message.nack()

def add_task(url):
    task_id = str(int(time.time()))  # simple task ID generation
    publish_task(url, task_id)
    time.sleep(1)

def publish_task(url, task_id):
    message = {"task_id": task_id, "url": url}
    publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    redis_client.hset(status_key, task_id, "sent")
    redis_client.hset(url_key, task_id, url)
    redis_client.hset(heartbeat_key, task_id, int(time.time()))
    print(f"[PUBLISH] Task {task_id} with URL {url}")

def listen_for_tasks():
    while True:
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_task)
        streaming_pull_future.result()

def monitor_heartbeat():
    while True:
        now = int(time.time())
        all_heartbeats = redis_client.hgetall(heartbeat_key)
        for task_id_bytes, last_beat_bytes in all_heartbeats.items():
            task_id = task_id_bytes.decode("utf-8")
            last_beat = int(last_beat_bytes.decode("utf-8"))
            if now - last_beat > TIMEOUT:
                url = redis_client.hget(url_key, task_id).decode("utf-8")
                publish_task(url, int(task_id))
        time.sleep(10)

def main():
    listen_for_tasks()

if __name__ == "__main__":
    main()
