"""Tests for the custom DQ framework."""

from datetime import datetime, timedelta, timezone

import pandas as pd

from company_intel.data_quality.checks import (
    freshness,
    null_rate,
    row_count_anomaly,
    schema_drift,
)


def test_row_count_anomaly_passes_within_sigma():
    df = pd.DataFrame({"x": range(100)})
    result = row_count_anomaly(df, table="t", baseline=[95, 98, 100, 102, 105], sigma=3.0)
    assert result.passed


def test_row_count_anomaly_flags_outlier():
    df = pd.DataFrame({"x": range(10_000)})
    result = row_count_anomaly(df, table="t", baseline=[95, 98, 100, 102, 105], sigma=3.0)
    assert not result.passed
    assert result.severity == "warning"


def test_freshness_passes_for_recent_data():
    now = datetime.now(timezone.utc)
    df = pd.DataFrame({"updated_at": [now - timedelta(minutes=5)]})
    result = freshness(df, table="t", ts_column="updated_at", max_age=timedelta(hours=1))
    assert result.passed


def test_freshness_fails_for_stale_data():
    now = datetime.now(timezone.utc)
    df = pd.DataFrame({"updated_at": [now - timedelta(days=2)]})
    result = freshness(df, table="t", ts_column="updated_at", max_age=timedelta(hours=1))
    assert not result.passed


def test_schema_drift_detects_added_column():
    current = {"a": "string", "b": "int", "c": "float"}
    expected = {"a": "string", "b": "int"}
    result = schema_drift(current_schema=current, expected_schema=expected, table="t")
    assert not result.passed
    assert "added" in result.message


def test_schema_drift_detects_type_change():
    current = {"a": "string", "b": "string"}
    expected = {"a": "string", "b": "int"}
    result = schema_drift(current_schema=current, expected_schema=expected, table="t")
    assert not result.passed
    assert "type_changed" in result.message


def test_null_rate_per_column():
    df = pd.DataFrame({"good": [1, 2, 3, 4], "bad": [None, None, None, 1]})
    results = null_rate(df, table="t", max_null_rate=0.1)
    by_col = {r.check_name: r for r in results}
    assert by_col["null_rate[good]"].passed
    assert not by_col["null_rate[bad]"].passed
