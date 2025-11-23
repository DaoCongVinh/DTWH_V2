FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y build-essential libssl-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY loader.py loader.sql date_dim.csv etl_processor_scd2.py manual_process_scd2.py check_status.py config.py app.py main_job.py ./

CMD ["python", "app.py"]


