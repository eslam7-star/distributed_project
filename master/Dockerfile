FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

ENV GOOGLE_APPLICATION_CREDENTIALS="/secrets/gcp-credentials.json"


CMD ["python3", "master_node.py"]
