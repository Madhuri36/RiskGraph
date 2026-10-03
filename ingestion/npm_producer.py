"""Ingest top-N npm packages and their transitive dependency closure into Kafka."""

import json
import os
import time
from collections import deque
from urllib.parse import quote

import requests
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REGISTRY = "https://registry.npmjs.org"
SEARCH_URL = f"{REGISTRY}/-/v1/search"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

KAFKA_TOPIC = os.getenv("NPM_TOPIC", "npm-packages")

TOP_N = int(os.getenv("TOP_N", "3000"))
MAX_NODES = int(os.getenv("MAX_NODES", "0"))

SEARCH_PAGE_SIZE = 250
REQUEST_DELAY = float(os.getenv("NPM_REQUEST_DELAY", "0.05"))

# Broad terms used to discover a large candidate pool.
SEARCH_TERMS = [
    "javascript",
    "node",
    "module",
    "npm",
    "react",
    "web",
    "api",
    "cli",
    "typescript",
    "plugin",
    "library",
    "server",
]


def create_session():
    """Create an HTTP session with retries."""

    session = requests.Session()

    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.5,
        status_forcelist=(
            429,
            500,
            502,
            503,
            504,
        ),
        allowed_methods=frozenset(["GET"]),
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)

    return session


session = create_session()


def discover_top_packages():
    """
    Discover popular npm packages.

    We collect candidates from several broad searches,
    deduplicate them, then rank them by monthly downloads.
    """

    candidates = {}

    for term in SEARCH_TERMS:

        print(f"Searching npm for: {term}")

        for offset in range(0, TOP_N, SEARCH_PAGE_SIZE):

            params = {
                "text": term,
                "size": SEARCH_PAGE_SIZE,
                "from": offset,
                "popularity": 1.0,
                "quality": 0.0,
                "maintenance": 0.0,
            }

            response = session.get(
                SEARCH_URL,
                params=params,
                timeout=30,
            )

            response.raise_for_status()

            objects = response.json().get("objects", [])

            if not objects:
                break

            for obj in objects:

                package = obj.get("package", {})

                name = package.get("name")

                if not name:
                    continue

                downloads = obj.get("downloads", {})

                monthly = downloads.get("monthly", 0) or 0

                candidates[name] = max(candidates.get(name, 0), monthly)

            if len(objects) < SEARCH_PAGE_SIZE:
                break

            time.sleep(REQUEST_DELAY)

    ranked = sorted(
        candidates.items(),
        key=lambda item: item[1],
        reverse=True,
    )

    selected = [name for name, _ in ranked[:TOP_N]]

    print(f"Discovered {len(candidates)} unique " f"candidate packages.")

    print(f"Selected {len(selected)} top packages.")

    return selected


def fetch_package(name):
    """
    Fetch metadata for one npm package.
    """

    encoded_name = quote(name, safe="@/")

    response = session.get(
        f"{REGISTRY}/{encoded_name}",
        headers={"Accept": "application/vnd.npm.install-v1+json"},
        timeout=30,
    )

    response.raise_for_status()

    metadata = response.json()

    latest_version = metadata.get("dist-tags", {}).get("latest")

    version_info = metadata.get("versions", {}).get(latest_version, {})

    return {
        "name": metadata.get("name", name),
        "version": latest_version,
        "description": metadata.get("description"),
        "dependencies": version_info.get("dependencies", {}),
        "devDependencies": version_info.get("devDependencies", {}),
        "maintainers": metadata.get("maintainers", []),
        "collected_at": int(time.time()),
        "source": "npm_registry",
    }


def dependency_names(record):
    """
    Extract dependency package names.

    We include both dependencies and
    devDependencies because your project
    models both as dependency relationships.
    """

    names = set(record.get("dependencies", {}).keys())

    names.update(record.get("devDependencies", {}).keys())

    return names


def publish_record(producer, record):
    """Publish one package to Kafka."""

    future = producer.send(KAFKA_TOPIC, value=record)

    metadata = future.get(timeout=30)

    print(
        f"Published "
        f"{record['name']}@{record['version']} "
        f"to partition "
        f"{metadata.partition}"
    )


def main():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
        acks="all",
    )

    try:

        # --------------------------------
        # PHASE 1: TOP N SEEDS
        # --------------------------------

        seeds = discover_top_packages()

        # --------------------------------
        # PHASE 2: DEPENDENCY CLOSURE
        # --------------------------------

        queue = deque(seeds)

        discovered = set()

        published = 0

        print(f"\nStarting dependency closure.")

        print(f"Seed packages: {len(seeds)}")

        print(f"Maximum nodes: {MAX_NODES}\n")

        while queue and (MAX_NODES == 0 or len(discovered) < MAX_NODES):

            name = queue.popleft()

            if name in discovered:
                continue

            discovered.add(name)

            try:

                record = fetch_package(name)

            except requests.RequestException as error:

                print(f"Skipping {name}: {error}")

                continue

            # Publish package metadata.
            publish_record(producer, record)

            published += 1

            # --------------------------------
            # ADD TRANSITIVE DEPENDENCIES
            # --------------------------------

            for dependency in dependency_names(record):

                if dependency not in discovered:

                    queue.append(dependency)

            # Progress information.
            if published % 100 == 0:

                print(
                    f"\nProgress:"
                    f" published={published},"
                    f" discovered={len(discovered)},"
                    f" queued={len(queue)}\n"
                )

            time.sleep(REQUEST_DELAY)

        producer.flush()

        print("\n==============================")

        print("INGESTION COMPLETE")

        print("==============================")

        print(f"Seed packages: {len(seeds)}")

        print(f"Packages discovered: " f"{len(discovered)}")

        print(f"Packages published: " f"{published}")

        if MAX_NODES > 0 and len(discovered) >= MAX_NODES:

            print("\nWARNING:")

            print("MAX_NODES was reached.")

            print("Increase MAX_NODES if you " "want a larger closure.")

    finally:

        producer.close()


if __name__ == "__main__":
    main()
