"""Tests for the Airflow DAG task logic.

Airflow itself isn't a project dependency, so we stub the `airflow` modules
in sys.modules before importing the DAG. This lets us exercise the pure-Python
task callables (scrape -> row mapping -> Postgres upsert) without an Airflow
install or a live database.
"""

import sys
import types
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

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
