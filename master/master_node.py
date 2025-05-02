import time
import json
import redis
import os
import threading
from google.cloud import pubsub_v1
from google.auth.transport.requests import Request
from google.auth import default

PROJECT_ID = "pure-karma-387207"
TOPIC_NAME = "crawl-tasks"
SUBSCRIPTION_NAME = "index-sub"
SEED_URLS = [
    "https://example.com",
    "https://google.com",
    "https://wikipedia.org"
]
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

crawler_last_seen = {}

def publish_task(url, task_id):
    message = {"task_id": task_id, "url": url}
    publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    redis_client.hset(status_key, task_id, "sent")
    redis_client.hset(url_key, task_id, url)
    redis_client.hset(heartbeat_key, task_id, int(time.time()))
    print(f"[PUBLISH] Task {task_id} with URL {url}")

def listen_for_results():
    def callback(message):
        try:
            if not message.data:
                message.ack()
                return
            data = json.loads(message.data.decode("utf-8"))
            if not data:
                message.ack()
                return
            task_id = str(data.get("task_id"))
            status = data.get("status", "unknown")
            crawler_id = data.get("crawler_id", "unknown")
            redis_client.hset(status_key, task_id, status)
            crawler_last_seen[crawler_id] = int(time.time())
            print(f"[RESULT] Task {task_id} - Status: {status} from {crawler_id}")
        except Exception as e:
            print(f"[ERROR] Failed to process message: {e}")
        message.ack()

    while True:
        try:
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
            print(f"[SUBSCRIBER] Listening for results on {subscription_path}...")
            streaming_pull_future.result()
        except Exception as e:
            print(f"[ERROR] Subscriber error, retrying in 5 seconds: {e}")
            time.sleep(5)

def monitor_heartbeat():
    while True:
        now = int(time.time())
        all_heartbeats = redis_client.hgetall(heartbeat_key)
        if not all_heartbeats:
            print("[MONITOR] No crawlers active or sending heartbeats.")
        for task_id_bytes, last_beat_bytes in all_heartbeats.items():
            task_id = task_id_bytes.decode("utf-8")
            last_beat = int(last_beat_bytes.decode("utf-8"))
            if now - last_beat > TIMEOUT:
                url = redis_client.hget(url_key, task_id)
                if url:
                    url = url.decode("utf-8")
                    print(f"[TIMEOUT] Task {task_id} timed out. Reassigning {url}.")
                    publish_task(url, int(task_id))
        time.sleep(10)

def log_stats():
    while True:
        try:
            status_counts = redis_client.hgetall(status_key)
            active_crawlers = [cid for cid, t in crawler_last_seen.items() if int(time.time()) - t < TIMEOUT]
            print("\n[MONITORING DASHBOARD]")
            print(f"Crawled: {list(status_counts.values()).count(b'done')}")
            print(f"Failed: {list(status_counts.values()).count(b'fail')}")
            print(f"In progress: {list(status_counts.values()).count(b'sent')}")
            print(f"Active Crawlers: {active_crawlers}\n")
        except Exception as e:
            print(f"[ERROR] Monitoring failed: {e}")
        time.sleep(20)

def main():
    task_id = 0
    for url in SEED_URLS:
        task_id += 1
        publish_task(url, task_id)
        time.sleep(1)

    threading.Thread(target=monitor_heartbeat, daemon=True).start()
    threading.Thread(target=log_stats, daemon=True).start()
    listen_for_results()

if __name__ == "__main__":
    main()
