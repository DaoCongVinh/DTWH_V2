FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (including netcat for health check)
RUN apt-get update && apt-get install -y \
    mariadb-client \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY config.py .
COPY db.py .
COPY loader.py .
COPY transformer.py .
COPY tiktok_schema.json .
COPY logging_setup.py .

# Create log directory
RUN mkdir -p /app/logs

# Environment settings
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Ho_Chi_Minh

# Create clean entrypoint script (no DateDim load)
RUN cat << 'EOF' > /app/entrypoint.sh
#!/bin/bash
set +e

echo "Waiting for MySQL..."
while ! nc -z db 3306; do sleep 1; done
echo "MySQL is ready"

echo "Starting TikTok Loader..."
echo "======== LOADER STAGING START ========"

exec python loader.py --schedule
EOF

RUN chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]
