"""Publish a small GH Archive sample for testing the event pipeline.
For full-scale collection, download hourly GH Archive files and stream parsed events.
"""

import os
import json
import logging
import requests

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Load variables from the project's root .env file
load_dotenv()

# Configuration
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "nodejs/node")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
)
KAFKA_TOPIC = os.getenv("GITHUB_EVENTS_TOPIC", "github-events")

GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/events"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def create_kafka_producer():
    """Connect to the Kafka broker and create a producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
        acks="all"
    )


def fetch_github_events():
    """Fetch recent public events from the configured GitHub repository."""
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "RiskGraph-Data-Producer"
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    else:
        logger.warning(
            "GITHUB_TOKEN is not set. Using unauthenticated API access."
        )

    try:
        response = requests.get(
            GITHUB_API_URL,
            headers=headers,
            timeout=30
        )

        if response.status_code == 403:
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset = response.headers.get("X-RateLimit-Reset")
            logger.error(
                "GitHub API rate limit or access restriction. "
                "Remaining: %s, Reset timestamp: %s",
                remaining,
                reset
            )
            response.raise_for_status()

        response.raise_for_status()
        events = response.json()

        if not isinstance(events, list):
            raise ValueError("Unexpected response from GitHub API")

        logger.info("Fetched %d GitHub events", len(events))
        return events

    except requests.exceptions.RequestException as error:
        logger.error("Failed to fetch GitHub events: %s", error)
        return []


def publish_events(producer, events):
    """Publish fetched GitHub events to the Kafka topic."""
    if not events:
        logger.warning("No events to publish.")
        return

    sent = 0

    for event in events:
        # Keep useful event information for downstream analytics
        record = {
            "event_id": event.get("id"),
            "event_type": event.get("type"),
            "repo": event.get("repo", {}).get("name"),
            "actor": event.get("actor", {}).get("login"),
            "created_at": event.get("created_at"),
            "payload": event.get("payload", {}),
            "source": "github"
        }

        try:
            future = producer.send(KAFKA_TOPIC, value=record)
            future.get(timeout=30)
            sent += 1
        except KafkaError as error:
            logger.error(
                "Failed to publish event %s: %s",
                record["event_id"],
                error
            )

    producer.flush()
    logger.info(
        "Published %d out of %d events to topic '%s'",
        sent,
        len(events),
        KAFKA_TOPIC
    )


def main():
    producer = None

    try:
        logger.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
        producer = create_kafka_producer()
        logger.info("Connected to Kafka.")

        events = fetch_github_events()
        publish_events(producer, events)

    except Exception:
        logger.exception("GitHub producer failed.")

    finally:
        if producer is not None:
            producer.close()
            logger.info("Kafka producer closed.")


if __name__ == "__main__":
    main()
