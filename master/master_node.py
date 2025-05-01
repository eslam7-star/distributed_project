import time
import json
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

def publish_task(url, task_id):
    message = json.dumps({"task_id": task_id, "url": url}).encode("utf-8")
    publisher.publish(topic_path, message)
    print(f"Published task {task_id} with URL {url}")

def listen_for_results():
    def callback(message):
        data = json.loads(message.data.decode("utf-8"))
        task_id = data["task_id"]
        url = data["url"]
        status = data["status"]
        print(f"Received result for task {task_id} - URL: {url} - Status: {status}")
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for results on {subscription_path}...")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Stopped listening for results.")

def monitor_heartbeat():
    task_timeout = {}
    while True:
        current_time = time.time()
        for task_id, last_time in task_timeout.items():
            if current_time - last_time > TIMEOUT:
                print(f"Task {task_id} timed out, rescheduling...")
                task_timeout[task_id] = current_time
        time.sleep(5)

def main():
    task_id = 0
    for url in SEED_URLS:
        task_id += 1
        publish_task(url, task_id)
        time.sleep(2)
    
    monitor_heartbeat()
    listen_for_results()

if __name__ == "__main__":
    main()
