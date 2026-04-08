#!/usr/bin/env python3
"""
kafka_to_volume.py — Local Kafka consumer that lands CDC events as NDJSON files
in a Databricks Unity Catalog Volume for Auto Loader ingestion.

Architecture:
  Kafka (local) → [this script] → /Volumes/.../bronze-landing/<table>/<date>/
    → Auto Loader (cloudFiles) → Bronze Delta tables

The file format mirrors the Kafka envelope exactly so the Bronze notebook
parsing logic is unchanged:
  {topic, partition, offset, timestamp (ms epoch), message_key, value}

Usage:
    # Source .env first for Databricks credentials
    set -a && source .env && set +a
    python3 scripts/kafka_to_volume.py

    # Override defaults
    python3 scripts/kafka_to_volume.py \\
        --bootstrap localhost:9092 \\
        --topic-pattern "^cdc\\.public\\..*" \\
        --landing-root /Volumes/workspace/default/mnt/bronze-landing \\
        --flush-records 500 \\
        --flush-seconds 30

    # Run via Docker Compose internal broker (no ngrok needed)
    docker compose --profile kafka-to-volume up -d
"""

import argparse
import json
import os
import re
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO

from confluent_kafka import Consumer, KafkaError
from databricks.sdk import WorkspaceClient

TOPIC_RE = re.compile(r"^[^.]+\.[^.]+\.([^.]+)$")  # cdc.schema.table → table


def _table_from_topic(topic: str) -> str | None:
    m = TOPIC_RE.match(topic)
    return m.group(1) if m else None


def _build_consumer(bootstrap: str, group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers":  bootstrap,
        "group.id":           group_id,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })


def _upload_batch(
    w: WorkspaceClient,
    landing_root: str,
    table_name: str,
    messages: list[dict],
) -> str:
    """Upload one NDJSON file to the Volume landing zone. Returns the remote path."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id   = uuid.uuid4().hex[:8]
    path     = f"{landing_root}/{table_name}/{date_str}/{run_id}.ndjson"

    ndjson = "\n".join(json.dumps(m) for m in messages) + "\n"
    w.files.upload(path, BytesIO(ndjson.encode()), overwrite=True)
    return path


def run(
    bootstrap: str,
    topic_pattern: str,
    landing_root: str,
    flush_records: int,
    flush_seconds: float,
    databricks_host: str,
    databricks_token: str,
) -> None:
    w        = WorkspaceClient(host=databricks_host, token=databricks_token)
    consumer = _build_consumer(bootstrap, group_id="kafka-to-volume")
    consumer.subscribe([topic_pattern])

    # buffer: table_name → list of message dicts
    buffers:   dict[str, list[dict]] = defaultdict(list)
    last_flush = time.monotonic()

    print(f"Consuming  {topic_pattern}  from  {bootstrap}")
    print(f"Uploading to  {landing_root}")
    print(f"Flush threshold: {flush_records} records  or  {flush_seconds}s")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now = time.monotonic()

            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        pass  # normal end-of-partition — keep polling
                    else:
                        print(f"ERROR: {msg.error()}", file=sys.stderr)
                else:
                    table = _table_from_topic(msg.topic())
                    if table:
                        buffers[table].append({
                            "topic":       msg.topic(),
                            "partition":   msg.partition(),
                            "offset":      msg.offset(),
                            "timestamp":   msg.timestamp()[1],  # ms epoch
                            "message_key": msg.key().decode("utf-8") if msg.key()   else None,
                            "value":       msg.value().decode("utf-8") if msg.value() else None,
                        })

            total_buffered = sum(len(v) for v in buffers.values())
            time_elapsed   = now - last_flush

            if total_buffered >= flush_records or (total_buffered > 0 and time_elapsed >= flush_seconds):
                _flush(w, landing_root, buffers, consumer)
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        remaining = {t: r for t, r in buffers.items() if r}
        if remaining:
            print("Flushing remaining buffers before exit...")
            _flush(w, landing_root, buffers, consumer)
    finally:
        consumer.close()
        print("Consumer closed.")


def _flush(
    w: WorkspaceClient,
    landing_root: str,
    buffers: dict[str, list[dict]],
    consumer: Consumer,
) -> None:
    for table, rows in list(buffers.items()):
        if not rows:
            continue
        path = _upload_batch(w, landing_root, table, rows)
        print(f"  → {path}  ({len(rows)} records)")
        buffers[table] = []
    consumer.commit()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic-pattern",
        default="^cdc\\.public\\..*",
        help="Kafka regex topic subscription pattern (default: ^cdc\\.public\\..*)",
    )
    parser.add_argument(
        "--landing-root",
        default="/Volumes/workspace/default/mnt/bronze-landing",
        help="Databricks Volume path for landing NDJSON files",
    )
    parser.add_argument(
        "--flush-records",
        type=int,
        default=500,
        help="Upload a batch after accumulating this many records (default: 500)",
    )
    parser.add_argument(
        "--flush-seconds",
        type=float,
        default=30.0,
        help="Upload a batch after this many seconds of inactivity (default: 30)",
    )
    args = parser.parse_args()

    host  = os.environ.get("DATABRICKS_HOST",  "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host or not token:
        parser.error("DATABRICKS_HOST and DATABRICKS_TOKEN must be set (source .env first)")

    run(
        bootstrap        = args.bootstrap,
        topic_pattern    = args.topic_pattern,
        landing_root     = args.landing_root,
        flush_records    = args.flush_records,
        flush_seconds    = args.flush_seconds,
        databricks_host  = host,
        databricks_token = token,
    )


if __name__ == "__main__":
    main()
