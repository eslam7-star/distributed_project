import time
import json
import threading
from google.cloud import pubsub_v1

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

def publish_to_dashboard():
    now = int(time.time())
    active_crawlers = [cid for cid, ts in crawler_last_seen.items() if now - ts <= TIMEOUT]
    failed_crawlers = [cid for cid, ts in crawler_last_seen.items() if now - ts > TIMEOUT]
    status_data = {
        "active_crawlers": active_crawlers,
        "failed_crawlers": failed_crawlers,
        "task_status": crawler_task_status,
        "crawled_urls": len([s for s in crawler_task_status.values() if s == "crawled"]),
        "indexed_urls": len([s for s in crawler_task_status.values() if s == "indexed"]),
        "error_count": len([s for s in crawler_task_status.values() if s == "error"]),
        "heartbeat_timestamps": crawler_last_seen
    }
    try:
        publisher.publish(dashboard_topic_path, json.dumps(status_data).encode("utf-8"))
        print("[DASHBOARD] Published status to UI")
    except Exception as e:
        print(f"[ERROR] Failed to publish to dashboard: {e}")

def listen_for_results():
    def callback(message):
        try:
            if not message.data:
                message.ack()
                return
            data = json.loads(message.data.decode("utf-8"))
            task_id = str(data.get("task_id"))
            status = data.get("status", "unknown")
            crawler_id = data.get("crawler_id", "unknown")
            url = data.get("url", f"task-{task_id}")

            crawler_task_status[url] = status
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

def dashboard_loop():
    while True:
        publish_to_dashboard()
        time.sleep(10)

def main():
    for i, url in enumerate(SEED_URLS, start=1):
        publish_task(url, i)
        time.sleep(1)

    threading.Thread(target=dashboard_loop, daemon=True).start()
    listen_for_results()

if __name__ == "__main__":
    main()
