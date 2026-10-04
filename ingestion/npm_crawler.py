
import json
import time
from collections import deque
from datetime import datetime, timezone
from urllib.parse import quote

import requests
from kafka import KafkaProducer

BOOTSTRAP = "localhost:29092"
TOPIC = "npm-packages"
TARGET = 4000
DELAY = 0.08

SEARCH_TERMS = [
    "react", "node", "typescript", "javascript", "express",
    "web", "database", "testing", "cli", "build",
    "security", "http", "logging", "parser", "babel",
    "webpack", "vite", "next", "vue", "angular",
    "css", "ui", "server", "api", "cloud"
]

session = requests.Session()
session.headers.update({"User-Agent": "RiskGraph-Research/1.0"})

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5
)

def search_packages(term, limit=250):
    names = []
    for offset in range(0, limit, 250):
        url = "https://registry.npmjs.org/-/v1/search"
        response = session.get(
            url,
            params={"text": term, "size": 250, "from": offset},
            timeout=30
        )
        response.raise_for_status()
        objects = response.json().get("objects", [])
        if not objects:
            break
        names.extend(
            item["package"]["name"]
            for item in objects
            if item.get("package", {}).get("name")
        )
        if len(objects) < 250:
            break
        time.sleep(DELAY)
    return names

def get_metadata(name):
    url = "https://registry.npmjs.org/" + quote(name, safe="@")
    for attempt in range(4):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            version = data.get("dist-tags", {}).get("latest")
            if not version:
                return None
            release = data.get("versions", {}).get(version, {})
            return {
                "name": data.get("name", name),
                "version": version,
                "description": release.get("description", data.get("description", "")),
                "dependencies": release.get("dependencies", {}),
                "devDependencies": release.get("devDependencies", {}),
                "maintainers": data.get("maintainers", []),
                "repository": release.get("repository", data.get("repository")),
                "collected_at": datetime.now(timezone.utc).isoformat()
            }
        except requests.RequestException:
            if attempt == 3:
                print(f"Failed: {name}")
                return None
            time.sleep(2 ** attempt)

def main():
    discovered = set()
    queue = deque()

    # Discover initial package names from npm search
    for term in SEARCH_TERMS:
        try:
            for name in search_packages(term):
                if name not in discovered:
                    discovered.add(name)
                    queue.append(name)
        except requests.RequestException as exc:
            print(f"Search failed for {term}: {exc}")
        if len(discovered) >= TARGET:
            break

    published = 0
    while queue and published < TARGET:
        name = queue.popleft()
        metadata = get_metadata(name)
        if metadata is None:
            continue

        future = producer.send(TOPIC, metadata)
        future.get(timeout=60)
        published += 1

        # Discover dependencies for further crawling
        for dep in metadata["dependencies"]:
            if dep not in discovered and len(discovered) < TARGET * 3:
                discovered.add(dep)
                queue.append(dep)

        if published % 100 == 0:
            producer.flush()
            print(f"Published {published}; queued {len(queue)}")

        time.sleep(DELAY)

    producer.flush()
    producer.close()
    print(f"Finished. Published {published} package records.")

if __name__ == "__main__":
    main()
