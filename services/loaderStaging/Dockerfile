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
COPY date_dim.csv .
COPY logging_setup.py .
COPY ensure_date_dim.py .

# Create log directory
RUN mkdir -p /app/logs

# Environment
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Ho_Chi_Minh

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
echo "================================"\n\
echo "LOADER STAGING STARTING"\n\
echo "================================"\n\
\n\
# Wait for MySQL to be ready\n\
echo "Waiting for MySQL..."\n\
until nc -z db 3306; do\n\
    echo "MySQL not ready, retrying..."\n\
    sleep 2\n\
done\n\
echo "✓ MySQL is ready"\n\
\n\
# Additional wait for init scripts to complete\n\
echo "Waiting for database initialization..."\n\
sleep 5\n\
\n\
# Ensure DateDim is ready\n\
echo "Ensuring DateDim is ready..."\n\
python ensure_date_dim.py\n\
\n\
# Start the scheduler\n\
echo ""\n\
echo "================================"\n\
echo "STARTING LOADER SCHEDULER"\n\
echo "================================"\n\
exec python loader.py --schedule\n\
' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

CMD ["/bin/bash", "/app/entrypoint.sh"]
