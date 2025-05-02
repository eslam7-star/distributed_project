import time
import json
import requests
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup
from urllib.parse import urljoin

PROJECT_ID = "pure-karma-387207"
TASK_TOPIC_NAME = "crawl-tasks"
RESULT_TOPIC_NAME = "crawl-results"
HEARTBEAT_TOPIC_NAME = "crawler-heartbeats"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
task_topic_path = publisher.topic_path(PROJECT_ID, TASK_TOPIC_NAME)
result_topic_path = publisher.topic_path(PROJECT_ID, RESULT_TOPIC_NAME)
heartbeat_topic_path = publisher.topic_path(PROJECT_ID, HEARTBEAT_TOPIC_NAME)

CRAWLER_ID = f"crawler-{int(time.time())}"
TIMEOUT = 60

def extract_links(soup, base_url):
    links = set()
    for a_tag in soup.find_all("a", href=True):
        link = a_tag.get("href")
        full_link = urljoin(base_url, link)
        links.add(full_link)
    return list(links)

def fetch_and_process_page(url, task_id):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, "html.parser")
            links = extract_links(soup, url)
            send_result(task_id, "crawled", url)
            return links
        else:
            send_result(task_id, "error", url)
    except Exception as e:
        send_result(task_id, "error", url)
    return []

def send_result(task_id, status, url):
    result_msg = {
        "task_id": task_id,
        "status": status,
        "crawler_id": CRAWLER_ID,
        "url": url
    }
    publisher.publish(result_topic_path, json.dumps(result_msg).encode("utf-8"))
    update_heartbeat()

def update_heartbeat():
    heartbeat_msg = {
        "crawler_id": CRAWLER_ID,
        "timestamp": int(time.time())
    }
    publisher.publish(heartbeat_topic_path, json.dumps(heartbeat_msg).encode("utf-8"))

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
        print(f"[ERROR] Task processing failed: {e}")
        message.nack()

def add_task(url):
    task_id = str(int(time.time() * 1000))
    publish_task(url, task_id)
    time.sleep(0.5)

def publish_task(url, task_id):
    message = {"task_id": task_id, "url": url}
    publisher.publish(task_topic_path, json.dumps(message).encode("utf-8"))
    print(f"[PUBLISH] Task {task_id} with URL {url}")

def listen_for_tasks():
    while True:
        streaming_pull_future = subscriber.subscribe(task_topic_path, callback=process_task)
        streaming_pull_future.result()

def main():
    listen_for_tasks()

if __name__ == "__main__":
    main()
