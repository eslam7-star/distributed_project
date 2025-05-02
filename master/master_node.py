import time
import json
import os
import threading
from google.cloud import pubsub_v1
from google.auth.transport.requests import Request
from google.auth import default

PROJECT_ID = "pure-karma-387207"
TASK_TOPIC_NAME = "crawl-tasks"
HEARTBEAT_TOPIC_NAME = "crawler-heartbeats"
RESULT_TOPIC_NAME = "crawl-results"
DASHBOARD_TOPIC_NAME = "crawler-dashboard"
SUBSCRIPTION_NAME = "index-sub"
UI_SUBSCRIPTION_NAME = "ui-sub"


SEED_URLS = [
    "https://example.com",
    "https://google.com",
    "https://wikipedia.org"
]
TIMEOUT = 60

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
task_topic_path = publisher.topic_path(PROJECT_ID, TASK_TOPIC_NAME)
heartbeat_topic_path = publisher.topic_path(PROJECT_ID, HEARTBEAT_TOPIC_NAME)
result_topic_path = publisher.topic_path(PROJECT_ID, RESULT_TOPIC_NAME)
dashboard_topic_path = publisher.topic_path(PROJECT_ID, DASHBOARD_TOPIC_NAME)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
ui_subscription_path = subscriber.subscription_path(PROJECT_ID, UI_SUBSCRIPTION_NAME)

crawler_last_seen = {}
crawler_task_status = {}

def publish_task(url, task_id):
    message = {"task_id": task_id, "url": url}
    publisher.publish(task_topic_path, json.dumps(message).encode("utf-8"))
    print(f"[PUBLISH] Task {task_id} with URL {url}")

def publish_heartbeat(task_id):
    heartbeat_message = {"task_id": task_id, "timestamp": int(time.time())}
    publisher.publish(heartbeat_topic_path, json.dumps(heartbeat_message).encode("utf-8"))
    print(f"[HEARTBEAT] Sent heartbeat for Task {task_id}")

def publish_to_dashboard():
    status_data = {
        "active_crawlers": list(crawler_last_seen.keys()),
        "task_status": crawler_task_status
    }
    publisher.publish(dashboard_topic_path, json.dumps(status_data).encode("utf-8"))
    print("[DASHBOARD] Published status data to dashboard")

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
            crawler_task_status[task_id] = status
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

def listen_for_ui_updates():
    def ui_callback(message):
        try:
            if not message.data:
                message.ack()
                return
            data = json.loads(message.data.decode("utf-8"))
            if not data:
                message.ack()
                return
            # Here you can process the data and forward it to your UI frontend
            print(f"[UI] Dashboard data received: {data}")
        except Exception as e:
            print(f"[ERROR] Failed to process UI message: {e}")
        message.ack()

    while True:
        try:
            streaming_pull_future = subscriber.subscribe(ui_subscription_path, callback=ui_callback)
            print(f"[UI SUBSCRIBER] Listening for dashboard updates...")
            streaming_pull_future.result()
        except Exception as e:
            print(f"[ERROR] UI Subscriber error, retrying in 5 seconds: {e}")
            time.sleep(5)

def monitor_heartbeat():
    while True:
        now = int(time.time())
        for crawler_id, last_seen in crawler_last_seen.items():
            if now - last_seen > TIMEOUT:
                print(f"[TIMEOUT] Crawler {crawler_id} timed out.")
        time.sleep(10)

def log_stats():
    while True:
        try:
            print("\n[MONITORING DASHBOARD]")
            print(f"Active Crawlers: {list(crawler_last_seen.keys())}\n")
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
    threading.Thread(target=listen_for_ui_updates, daemon=True).start()
    listen_for_results()

if __name__ == "__main__":
    main()
