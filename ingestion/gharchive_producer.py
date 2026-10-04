
import gzip
import json
import time
from datetime import date, datetime, timedelta, timezone

import requests
from kafka import KafkaProducer

BOOTSTRAP = "localhost:29092"
TOPIC = "github-events"

START_DATE = date(2026, 9, 26)
END_DATE = date(2026, 10, 3)  # exclusive: 7 days

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5
)

session = requests.Session()
session.headers.update({"User-Agent": "RiskGraph-Research/1.0"})

def ingest_hour(day, hour):
    url = f"https://data.gharchive.org/{day.isoformat()}-{hour}.json.gz"
    count = 0

    with session.get(url, stream=True, timeout=(20, 180)) as response:
        response.raise_for_status()
        response.raw.decode_content = True

        with gzip.GzipFile(fileobj=response.raw, mode="rb") as archive:
            for raw_line in archive:
                if not raw_line.strip():
                    continue
                event = json.loads(raw_line)
                producer.send(TOPIC, event)
                count += 1

                if count % 1000 == 0:
                    producer.flush()
                    print(f"{day} {hour:02d}:00 UTC: {count} events")

    producer.flush()
    print(f"Completed {day} {hour:02d}:00 UTC — {count} events")
    return count

def main():
    total = 0
    day = START_DATE

    while day < END_DATE:
        for hour in range(24):
            url = f"https://data.gharchive.org/{day.isoformat()}-{hour}.json.gz"
            try:
                total += ingest_hour(day, hour)
            except Exception as exc:
                print(f"FAILED {url}: {exc}")
                # Continue so a single failed hour does not stop the run.
            time.sleep(0.2)
        day += timedelta(days=1)

    producer.flush()
    producer.close()
    print(f"Total published events: {total}")

if __name__ == "__main__":
    main()
