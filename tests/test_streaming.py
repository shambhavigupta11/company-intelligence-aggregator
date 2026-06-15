"""Tests for the multi-source streaming registry and Kafka producer.

These exercise the pure declaration/transformation logic (stream specs, topic
map, batch publishing) without standing up Spark or a live Kafka broker.
"""

from unittest.mock import MagicMock

from mosaic.streaming import kafka_producer
from mosaic.streaming.spark_streaming_job import STREAM_SPECS


def test_stream_specs_are_well_formed():
    names = [s.name for s in STREAM_SPECS]
    topics = [s.topic for s in STREAM_SPECS]
    assert len(names) == len(set(names)), "stream spec names must be unique"
    assert len(topics) == len(set(topics)), "stream spec topics must be unique"


def test_watermark_columns_exist_in_schema():
    for spec in STREAM_SPECS:
        if spec.watermark_col is not None:
            fields = {f.name for f in spec.schema.fields}
            assert spec.watermark_col in fields, f"{spec.name} watermark not in schema"


def test_bronze_and_checkpoint_paths_are_per_source():
    paths = {s.bronze_path for s in STREAM_SPECS}
    checkpoints = {s.checkpoint_path for s in STREAM_SPECS}
    assert len(paths) == len(STREAM_SPECS)
    assert len(checkpoints) == len(STREAM_SPECS)


def test_producer_topics_cover_all_streamed_sources():
    # Every topic the streaming job consumes must be produced to by some source.
    produced = set(kafka_producer.TOPICS.values())
    consumed = {s.topic for s in STREAM_SPECS}
    assert consumed <= produced, f"unproduced topics: {consumed - produced}"


def test_publish_events_sends_each_with_key_and_flushes():
    fake = MagicMock()
    events = [{"full_name": "a/b", "stars": 1}, {"full_name": "c/d", "stars": 2}]
    n = kafka_producer.publish_events("github.repos", events, key_field="full_name", producer=fake)

    assert n == 2
    assert fake.send.call_count == 2
    # Key is taken from key_field and bytes-encoded.
    first_kwargs = fake.send.call_args_list[0].kwargs
    assert first_kwargs["key"] == b"a/b"
    assert first_kwargs["value"] == events[0]
    fake.flush.assert_called_once()


def test_publish_events_without_key_field():
    fake = MagicMock()
    n = kafka_producer.publish_events("hn.stories", [{"title": "x"}], producer=fake)
    assert n == 1
    assert fake.send.call_args.kwargs["key"] is None
