FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

COPY secrets/gcp-credentials.json gcp-credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS="gcp-credentials.json"


CMD ["python3", "crawler_node.py"]
