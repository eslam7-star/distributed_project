import time
import json
from google.cloud import pubsub_v1
from flask import Flask, render_template, jsonify
import threading

PROJECT_ID = "pure-karma-387207"
DASHBOARD_TOPIC_NAME = "crawler-dashboard"
UI_SUBSCRIPTION_NAME = "ui-sub"

app = Flask(__name__)

dashboard_data = {
    "active_crawlers": [],
    "failed_crawlers": [],
    "task_status": {},
    "crawled_urls": 0,
    "indexed_urls": 0,
    "error_count": 0,
    "heartbeat_timestamps": {}
}

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
dashboard_topic_path = publisher.topic_path(PROJECT_ID, DASHBOARD_TOPIC_NAME)
ui_subscription_path = subscriber.subscription_path(PROJECT_ID, UI_SUBSCRIPTION_NAME)

def listen_to_dashboard():
    def callback(message):
        global dashboard_data
        try:
            dashboard_data = json.loads(message.data.decode("utf-8"))
            print(f"[UI] Received update: {dashboard_data}")
        except Exception as e:
            print(f"[ERROR] Failed to process dashboard update: {e}")
        message.ack()

    while True:
        try:
            streaming_pull_future = subscriber.subscribe(ui_subscription_path, callback=callback)
            streaming_pull_future.result()
        except Exception as e:
            print(f"[ERROR] Subscriber error: {e}")
            time.sleep(5)

@app.route('/')
def dashboard():
    return render_template('dashboard.html', data=dashboard_data)

if __name__ == "__main__":
    threading.Thread(target=listen_to_dashboard, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
