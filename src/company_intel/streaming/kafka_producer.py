"""Kafka producer — publishes scraped events to topics for downstream stream processing."""

import json
import os
from typing import Any

from kafka import KafkaProducer

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def get_producer(bootstrap_servers: str = DEFAULT_BOOTSTRAP) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3,
    )


def publish_event(topic: str, event: dict[str, Any], key: str | None = None) -> None:
    """Publish a single event to a Kafka topic. Blocks until ack."""
    producer = get_producer()
    future = producer.send(topic, value=event, key=key.encode() if key else None)
    future.get(timeout=10)
    producer.flush()
