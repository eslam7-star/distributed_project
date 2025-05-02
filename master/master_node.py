import time
import json
import redis
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

redis_client = redis.Redis(host="redis", port=6379, db=0)

def publish_task(url, task_id):
    message = {"task_id": task_id, "url": url}
    publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    redis_client.hset("task_status", task_id, "sent")
    redis_client.hset("task_urls", task_id, url)
    redis_client.hset("task_heartbeat", task_id, int(time.time()))
    print(f"Published task {task_id} with URL {url}")

def listen_for_results():
    def callback(message):
        data = json.loads(message.data.decode("utf-8"))
        task_id = str(data["task_id"])
        status = data["status"]
        redis_client.hset("task_status", task_id, status)
        print(f"Received result for task {task_id} - Status: {status}")
        message.ack()
    subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for results on {subscription_path}...")
    while True:
        time.sleep(60)

def monitor_heartbeat():
    while True:
        now = int(time.time())
        all_heartbeats = redis_client.hgetall("task_heartbeat")
        for task_id_bytes, last_beat_bytes in all_heartbeats.items():
            task_id = task_id_bytes.decode("utf-8")
            last_beat = int(last_beat_bytes.decode("utf-8"))
            if now - last_beat > TIMEOUT:
                url = redis_client.hget("task_urls", task_id).decode("utf-8")
                print(f"Task {task_id} timed out, rescheduling...")
                publish_task(url, int(task_id))
        time.sleep(10)

def main():
    task_id = 0
    for url in SEED_URLS:
        task_id += 1
        publish_task(url, task_id)
        time.sleep(1)

    import threading
    threading.Thread(target=monitor_heartbeat, daemon=True).start()
    listen_for_results()

if __name__ == "__main__":
    main()
