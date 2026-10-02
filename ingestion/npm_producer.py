"""Fetch npm metadata for selected packages and publish it to Kafka."""
import json
import time
import requests
from kafka import KafkaProducer

PACKAGES = ["express", "react", "lodash", "debug", "chalk"]
REGISTRY = "https://registry.npmjs.org/{}"

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    retries=5,
)
for name in PACKAGES:
    response = requests.get(REGISTRY.format(name), timeout=20)
    response.raise_for_status()
    metadata = response.json()
    latest_version = metadata.get("dist-tags", {}).get("latest")
    version_info = metadata.get("versions", {}).get(latest_version, {})
    event = {
        "name": metadata.get("name", name),
        "version": latest_version,
        "description": metadata.get("description"),
        "dependencies": version_info.get("dependencies", {}),
        "devDependencies": version_info.get("devDependencies", {}),
        "maintainers": metadata.get("maintainers", []),
        "collected_at": int(time.time()),
        "source": "npm_registry",
    }
    future = producer.send("npm-packages", value=event)
    metadata = future.get(timeout=30)
    print(f"Published {name}@{latest_version} to partition {metadata.partition}")
producer.flush()
producer.close()
