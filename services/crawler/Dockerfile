FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y build-essential libssl-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install cryptography && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["python", "app.py"]

