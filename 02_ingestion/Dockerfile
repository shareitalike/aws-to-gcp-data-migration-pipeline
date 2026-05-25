FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY validate_landing.py .

# Use functions-framework to serve the cloud_event function
ENV PORT=8080
EXPOSE 8080

CMD ["functions-framework", "--target", "process_gcs_event", "--signature-type", "cloudevent"]
