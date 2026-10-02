import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "npm-packages", "github-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    consumer_timeout_ms=10000,
)
for message in consumer:
    print(message.topic, message.value)
consumer.close()
