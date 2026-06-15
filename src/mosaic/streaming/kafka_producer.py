"""Kafka producer — publishes scraped events to topics for downstream stream processing."""

import json
import os
from typing import Any

from collections.abc import Iterable

from kafka import KafkaProducer

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Source -> Kafka topic. Kept in sync with the consumer's STREAM_SPECS so
# producers and the streaming job agree on topic names.
TOPICS = {
    "github": "github.repos",
    "hn": "hn.stories",
    "reddit": "reddit.posts",
    "jobs": "jobs.listings",
}


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


def publish_events(
    topic: str,
    events: Iterable[dict[str, Any]],
    key_field: str | None = None,
    producer: KafkaProducer | None = None,
) -> int:
    """Publish many events to a topic over a single producer. Returns the count.

    Pass ``key_field`` to partition by a record field (e.g. ``"full_name"``).
    A shared ``producer`` may be supplied for reuse/testing; otherwise one is
    created and the events are flushed before returning.
    """
    prod = producer or get_producer()
    count = 0
    for event in events:
        key = str(event[key_field]).encode() if key_field else None
        prod.send(topic, value=event, key=key)
        count += 1
    prod.flush()
    return count
