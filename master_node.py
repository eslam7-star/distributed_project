from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("pure-karma-387207 ", "crawl-tasks")

urls = [
    "https://example.com",
    "https://wikipedia.org",
    "https://python.org"
]

for url in urls:
    future = publisher.publish(topic_path, data=url.encode("utf-8"))
    print(f" Published: {url}")
