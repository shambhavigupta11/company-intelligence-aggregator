"""DQ result reporters: console, Kafka topic (for dashboard alerts), Postgres log."""

import json
from dataclasses import asdict

from company_intel.data_quality.checks import DQResult
from company_intel.streaming.kafka_producer import publish_event

DQ_ALERTS_TOPIC = "dq.alerts"


def report_console(results: list[DQResult]) -> None:
    for r in results:
        marker = "✓" if r.passed else "✗"
        print(f"  [{marker}] {r.severity:7s} {r.check_name:30s} {r.table:30s} — {r.message}")


def report_kafka(results: list[DQResult]) -> None:
    """Publish failing checks to the dq.alerts Kafka topic for downstream consumers."""
    for r in results:
        if not r.passed:
            publish_event(DQ_ALERTS_TOPIC, json.loads(json.dumps(asdict(r), default=str)))
