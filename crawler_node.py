from google.cloud import pubsub_v1
import requests

def callback(message):
    url = message.data.decode("utf-8")
    print(f"[Crawler] Crawling: {url}")
    try:
        res = requests.get(url, timeout=5)
        print(f"[Crawler] Done: {url} → {res.status_code}")
    except Exception as e:
        print(f"[Crawler] Failed: {url} → {e}")
    message.ack()

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path("pure-karma-387207", "crawl-sub")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(" Crawler is listening for URLs...")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
