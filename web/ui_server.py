from flask import Flask, render_template, request, jsonify
from google.cloud import pubsub_v1
import json
import threading
from elasticsearch import Elasticsearch

PROJECT_ID = "pure-karma-387207"
SUBSCRIPTION_ID = "ui-sub"

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

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# Listen to Pub/Sub dashboard messages
def listen_to_dashboard():
    def callback(message):
        global dashboard_data
        try:
            data = json.loads(message.data.decode("utf-8"))

            for key in dashboard_data:
                if key in data:
                    dashboard_data[key] = data[key]

            print("[UI] Dashboard data received:", dashboard_data)
        except Exception as e:
            print(f"[ERROR] Failed to process dashboard message: {e}")
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)
    print(f"[UI SUBSCRIBER] Listening for dashboard updates on {SUBSCRIPTION_ID}...")

# Start background listener
threading.Thread(target=listen_to_dashboard, daemon=True).start()

@app.route("/")
def index():
    return render_template("index.html", data=dashboard_data)

@app.route("/status")
def status():
    return jsonify(dashboard_data)

# ElasticSearch connection
es = Elasticsearch("https://your-es-cloud-url", basic_auth=("elastic", "your_password"))

@app.route("/search")
def search():
    query = request.args.get("q")
    if not query:
        return "Missing query", 400

    result = es.search(index="crawled-data", query={"match": {"content": query}})
    hits = result["hits"]["hits"]
    return render_template("search.html", hits=hits, query=query)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
