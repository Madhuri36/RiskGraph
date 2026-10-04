
import json
import os
from datetime import datetime, timezone
from io import BytesIO

from kafka import KafkaConsumer
from minio import Minio

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY", "riskgraph")
SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY", "riskgraph_local_password")
ENDPOINT = os.getenv("RUSTFS_ENDPOINT", "localhost:9000")
BUCKET = os.getenv("RUSTFS_BUCKET", "riskgraph")

TOPICS = ["npm-packages", "github-events"]

client = Minio(
    ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP,
    group_id="riskgraph-rustfs-sink",
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    max_poll_records=500
)

def persist_batch(topic, records):
    if not records:
        return

    now = datetime.now(timezone.utc)
    day = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")

    grouped = {}
    for record in records:
        grouped.setdefault(record.partition, []).append(record)

    for partition, items in grouped.items():
        items.sort(key=lambda item: item.offset)
        first_offset = items[0].offset
        last_offset = items[-1].offset

        content = b"".join(
            (json.dumps(item.value, ensure_ascii=False) + "\n").encode("utf-8")
            for item in items
        )

        object_name = (
            f"raw/{topic}/ingest_date={day}/ingest_hour={hour}/"
            f"partition={partition}/offsets={first_offset}-{last_offset}.jsonl"
        )

        client.put_object(
            BUCKET,
            object_name,
            BytesIO(content),
            length=len(content),
            content_type="application/x-ndjson"
        )
        print(f"Stored {len(items)} records: {object_name}")

try:
    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=500)
        if not polled:
            continue

        for topic_partition, records in polled.items():
            persist_batch(topic_partition.topic, records)

        # Commit only after every batch from this poll was stored.
        consumer.commit()

except KeyboardInterrupt:
    print("Stopping RustFS sink...")

finally:
    consumer.close()
