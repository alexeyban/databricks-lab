FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# Local JSONL landing buffer, uploaded then moved to data/pumpapi/<date>/uploaded/
# by app.uploader; mount this as a volume to persist/inspect it outside the container.
VOLUME ["/app/data"]

ENV OUTPUT_DIR=/app/data/pumpapi

CMD ["python", "-m", "app.main"]
