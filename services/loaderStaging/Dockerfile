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
COPY load_date_dim_cli.py .
COPY tiktok_schema.json .
COPY date_dim.csv .
COPY logging_setup.py .

# Create log directory
RUN mkdir -p /app/logs

# Environment
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Ho_Chi_Minh

# Entrypoint script to load DateDim and then run loader
RUN echo '#!/bin/bash\nset +e\necho "Waiting for MySQL..."\nwhile ! nc -z db 3306; do sleep 1; done\necho "MySQL is ready"\nsleep 2\necho "Loading DateDim..."\npython load_date_dim_cli.py 2>&1 | head -20\necho "DateDim load completed (or skipped if error)"\necho "Starting Loader..."\necho "======== LOADER STAGING START ========"\nexec python loader.py' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Run entrypoint
CMD ["/app/entrypoint.sh"]
