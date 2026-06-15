"""Tests for the Airflow DAG task logic.

Airflow itself isn't a project dependency, so we stub the `airflow` modules
in sys.modules before importing the DAG. This lets us exercise the pure-Python
task callables (scrape -> row mapping -> Postgres upsert) without an Airflow
install or a live database.
"""

import sys
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def mosaic_dag(monkeypatch):
    """Import mosaic_dag with airflow stubbed out."""
    airflow = types.ModuleType("airflow")

    class _DAG:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    airflow.DAG = _DAG  # type: ignore[attr-defined]

    operators = types.ModuleType("airflow.operators")
    python_mod = types.ModuleType("airflow.operators.python")

    class _PythonOperator:
        def __init__(self, *a, **k):
            pass

        def __rshift__(self, other):
            return other

    python_mod.PythonOperator = _PythonOperator  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "airflow", airflow)
    monkeypatch.setitem(sys.modules, "airflow.operators", operators)
    monkeypatch.setitem(sys.modules, "airflow.operators.python", python_mod)
    sys.modules.pop("mosaic.batch.airflow_dags.mosaic_dag", None)

    from mosaic.batch.airflow_dags import mosaic_dag as mod

    return mod


def test_refresh_github_maps_rows_and_upserts(mosaic_dag):
    repo = MagicMock(
        full_name="databricks/delta",
        name="delta",
        description="lakehouse",
        language="Scala",
        stars=100,
        forks=10,
        open_issues=5,
        pushed_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    with patch.object(mosaic_dag, "scrape_github_org", return_value=[repo]), patch.object(
        mosaic_dag, "TRACKED_ORGS", ["databricks"]
    ), patch.object(
        mosaic_dag.postgres_writer, "upsert_github_repos", return_value=1
    ) as mock_upsert, patch.object(mosaic_dag, "_write_bronze") as mock_bronze:
        mosaic_dag.refresh_github_for_all_orgs()

    rows = mock_upsert.call_args.args[0]
    assert rows[0]["full_name"] == "databricks/delta"
    assert rows[0]["org"] == "databricks"
    assert rows[0]["stars"] == 100
    mock_bronze.assert_called_once()


def test_refresh_hn_dedupes_on_url(mosaic_dag):
    story_a = MagicMock(
        url="https://example.com/a",
        title="A",
        points=1,
        num_comments=2,
        author="x",
        created_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    # duplicate url -> should be collapsed before upsert
    story_dupe = MagicMock(
        url="https://example.com/a",
        title="A again",
        points=3,
        num_comments=4,
        author="y",
        created_at=datetime(2026, 5, 2, tzinfo=timezone.utc),
    )
    with patch.object(
        mosaic_dag, "scrape_hn_company_mentions", return_value=[story_a, story_dupe]
    ), patch.object(mosaic_dag, "TRACKED_ORGS", ["databricks"]), patch.object(
        mosaic_dag.postgres_writer, "upsert_hn_stories", return_value=1
    ) as mock_upsert, patch.object(mosaic_dag, "_write_bronze"):
        mosaic_dag.refresh_hn_mentions()

    rows = mock_upsert.call_args.args[0]
    assert len(rows) == 1
    assert rows[0]["story_url"] == "https://example.com/a"


def test_write_bronze_empty_is_noop(mosaic_dag):
    # No records -> returns without importing pyspark.
    mosaic_dag._write_bronze([], "github_repos")


def test_run_dq_checks_persists_results(mosaic_dag):
    gh = pd.DataFrame(
        {
            "full_name": ["databricks/delta"],
            "stars": [100],
            "pushed_at": [datetime.now(timezone.utc)],
        }
    )
    hn = pd.DataFrame(
        {
            "story_url": ["https://example.com/a"],
            "points": [10],
            "created_at": [datetime.now(timezone.utc)],
        }
    )
    with patch.object(mosaic_dag, "TRACKED_ORGS", ["databricks"]), patch.object(
        mosaic_dag.postgres_writer, "query_github_repos", return_value=gh
    ), patch.object(
        mosaic_dag.postgres_writer, "query_hn_stories", return_value=hn
    ), patch.object(
        mosaic_dag.postgres_writer, "insert_dq_alerts", return_value=0
    ) as mock_insert:
        mosaic_dag.run_dq_checks()

    rows = mock_insert.call_args.args[0]
    # Every persisted row carries the DQResult fields the alerts table expects.
    assert rows, "expected DQ results to be persisted"
    assert {"check_name", "table", "passed", "severity", "message", "measured_at"} <= rows[0].keys()
    # Recent pushed_at/created_at within the SLA -> freshness should pass.
    freshness_rows = [r for r in rows if r["check_name"] == "freshness"]
    assert freshness_rows and all(r["passed"] for r in freshness_rows)


def test_run_dq_checks_flags_stale_data(mosaic_dag):
    stale = datetime.now(timezone.utc) - timedelta(days=30)
    gh = pd.DataFrame({"full_name": ["x/y"], "stars": [1], "pushed_at": [stale]})
    hn = pd.DataFrame({"story_url": ["u"], "points": [1], "created_at": [stale]})
    with patch.object(mosaic_dag, "TRACKED_ORGS", ["x"]), patch.object(
        mosaic_dag.postgres_writer, "query_github_repos", return_value=gh
    ), patch.object(
        mosaic_dag.postgres_writer, "query_hn_stories", return_value=hn
    ), patch.object(
        mosaic_dag.postgres_writer, "insert_dq_alerts", return_value=0
    ) as mock_insert:
        mosaic_dag.run_dq_checks()

    rows = mock_insert.call_args.args[0]
    freshness_rows = [r for r in rows if r["check_name"] == "freshness"]
    assert freshness_rows and all(not r["passed"] for r in freshness_rows)


def test_build_company_signals_joins_and_aggregates(mosaic_dag):
    github_df = pd.DataFrame(
        {
            "full_name": ["databricks/delta", "databricks/spark", "apache/airflow"],
            "org": ["databricks", "databricks", "apache"],
            "stars": [100, 50, 200],
            "forks": [10, 5, 20],
            "open_issues": [1, 2, 3],
        }
    )
    hn_df = pd.DataFrame(
        {
            "story_url": ["a", "b"],
            "company": ["databricks", "databricks"],
            "points": [10, 5],
        }
    )
    gold = mosaic_dag.build_company_signals(github_df, hn_df)
    by_org = {r["org"]: r for r in gold.to_dict(orient="records")}

    assert by_org["databricks"]["repo_count"] == 2
    assert by_org["databricks"]["total_stars"] == 150
    assert by_org["databricks"]["hn_mentions"] == 2
    assert by_org["databricks"]["hn_points"] == 15
    # apache has GitHub data but no HN mentions -> filled with 0, not NaN.
    assert by_org["apache"]["hn_mentions"] == 0
    assert by_org["apache"]["total_stars"] == 200


def test_build_company_signals_handles_empty_inputs(mosaic_dag):
    gold = mosaic_dag.build_company_signals(pd.DataFrame(), pd.DataFrame())
    assert gold.empty


def test_publish_bigquery_skips_without_project(mosaic_dag, monkeypatch, capsys):
    monkeypatch.delenv("BIGQUERY_PROJECT_ID", raising=False)
    df = pd.DataFrame({"org": ["x"], "total_stars": [1]})
    # Should short-circuit before importing the BigQuery client.
    mosaic_dag._publish_bigquery(df, "company_signals")
    assert "skipping" in capsys.readouterr().out
