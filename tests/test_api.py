"""Tests for the Flask API routes (network/DB mocked)."""

from unittest.mock import patch

import pandas as pd

from mosaic.api.app import create_app


def _client():
    app = create_app()
    app.testing = True
    return app.test_client()


def test_health():
    resp = _client().get("/health")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}


@patch("mosaic.api.routes.postgres_writer.query_recent_dq_failures")
def test_alerts_reads_from_postgres(mock_query):
    mock_query.return_value = pd.DataFrame(
        [
            {
                "check_name": "freshness",
                "table": "github_repos",
                "passed": False,
                "severity": "error",
                "message": "stale",
                "measured_at": pd.Timestamp("2026-05-30T12:00:00Z"),
            }
        ]
    )
    resp = _client().get("/api/alerts?limit=5")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]["check_name"] == "freshness"
    assert data[0]["passed"] is False
    # timestamp serialized to a string (JSON-safe)
    assert isinstance(data[0]["measured_at"], str)
    mock_query.assert_called_once_with(limit=5)


@patch("mosaic.api.routes.postgres_writer.query_recent_dq_failures")
def test_alerts_empty(mock_query):
    mock_query.return_value = pd.DataFrame(
        columns=["check_name", "table", "passed", "severity", "message", "measured_at"]
    )
    resp = _client().get("/api/alerts")
    assert resp.status_code == 200
    assert resp.get_json() == []
