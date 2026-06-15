"""Airflow DAG: daily company intelligence refresh.

Scheduled to run nightly. Refreshes GitHub org metrics and HN mentions for
the tracked-companies list, lands them in Postgres (operational store) and
Delta bronze, runs the data-quality framework over the freshly written data,
then publishes a per-company gold aggregate to Delta gold and BigQuery.

Task flow: refresh_github -> refresh_hn -> run_dq_checks -> publish_gold
"""

import os
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from mosaic.data_quality import checks
from mosaic.scrapers.github_api import scrape_github_org
from mosaic.scrapers.hackernews_api import scrape_hn_company_mentions
from mosaic.storage import postgres_writer

DEFAULT_ARGS = {
    "owner": "mosaic",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

TRACKED_ORGS = ["databricks", "snowflake-labs", "apache", "dbt-labs"]

BRONZE_ROOT = os.environ.get("BRONZE_PATH_ROOT", "./data/bronze")
GOLD_ROOT = os.environ.get("GOLD_PATH_ROOT", "./data/gold")

# Per-table freshness SLA. The daily DAG should always land data newer than this.
FRESHNESS_SLA = timedelta(days=2)


def _write_bronze(records: list[dict], table: str) -> None:
    """Land raw records to a bronze Delta table.

    PySpark/Delta are heavy and only needed when actually writing, so the
    SparkSession is built lazily inside the task. If Spark isn't available
    (e.g. a lightweight Airflow worker), we skip the bronze write rather than
    failing the whole DAG — Postgres remains the source of truth for serving.
    """
    if not records:
        return
    try:
        from pyspark.sql import SparkSession

        from mosaic.storage.delta_writer import write_bronze
    except ImportError as exc:  # pragma: no cover - depends on worker env
        print(f"[bronze] skipping Delta write for {table}: pyspark unavailable ({exc})")
        return

    spark = (
        SparkSession.builder.appName("mosaic-batch-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    df = spark.createDataFrame(records)
    write_bronze(df, f"{BRONZE_ROOT}/{table}")
    print(f"[bronze] wrote {len(records)} rows to {BRONZE_ROOT}/{table}")


def refresh_github_for_all_orgs(**_: object) -> None:
    for org in TRACKED_ORGS:
        repos = scrape_github_org(org=org, limit=30)
        rows = [
            {
                "full_name": r.full_name,
                "org": org,
                "name": r.name,
                "description": r.description,
                "language": r.language,
                "stars": r.stars,
                "forks": r.forks,
                "open_issues": r.open_issues,
                "pushed_at": r.pushed_at,
            }
            for r in repos
        ]
        written = postgres_writer.upsert_github_repos(rows)
        _write_bronze(rows, "github_repos")
        print(f"[github] org={org} repos={len(repos)} postgres_rows={written}")


def refresh_hn_mentions(**_: object) -> None:
    for org in TRACKED_ORGS:
        stories = scrape_hn_company_mentions(company=org, limit=20)
        rows = [
            {
                # HN stories are keyed by URL in Postgres; fall back to the
                # HN item discussion URL when a story has no external link.
                "story_url": s.url or f"https://news.ycombinator.com/item?{s.title}",
                "company": org,
                "title": s.title,
                "points": s.points,
                "num_comments": s.num_comments,
                "author": s.author,
                "created_at": s.created_at,
            }
            for s in stories
        ]
        # Deduplicate on the conflict key so a single upsert batch can't collide.
        deduped = list({row["story_url"]: row for row in rows}.values())
        written = postgres_writer.upsert_hn_stories(deduped)
        _write_bronze(deduped, "hn_stories")
        print(f"[hn] org={org} stories={len(stories)} postgres_rows={written}")


def run_dq_checks(**_: object) -> None:
    """Run the DQ framework over the freshly written data and log results.

    Reads each tracked org's data back from Postgres (the silver/operational
    store), runs row-count, null-rate, and freshness checks per table, and
    persists every result to the `dq_alerts` table. The Flask API serves the
    failures from there, so DQ findings surface on the dashboard.
    """
    results = []
    for org in TRACKED_ORGS:
        gh = postgres_writer.query_github_repos(org=org, limit=1000)
        results.append(checks.row_count_anomaly(gh, table="github_repos", baseline=[]))
        results.extend(checks.null_rate(gh, table="github_repos", max_null_rate=0.5))
        results.append(
            checks.freshness(gh, table="github_repos", ts_column="pushed_at", max_age=FRESHNESS_SLA)
        )

        hn = postgres_writer.query_hn_stories(company=org, limit=1000)
        results.append(checks.row_count_anomaly(hn, table="hn_stories", baseline=[]))
        results.extend(checks.null_rate(hn, table="hn_stories", max_null_rate=0.5))
        results.append(
            checks.freshness(hn, table="hn_stories", ts_column="created_at", max_age=FRESHNESS_SLA)
        )

    rows = [asdict(r) for r in results]
    written = postgres_writer.insert_dq_alerts(rows)
    failures = sum(1 for r in results if not r.passed)
    print(f"[dq] checks={len(results)} failures={failures} alerts_written={written}")


def build_company_signals(github_df: pd.DataFrame, hn_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-org GitHub + HN signals into a single gold row per company.

    Pure pandas (no Spark/DB) so it's cheap to unit-test. Joins on org/company.
    """
    if github_df.empty:
        gh_agg = pd.DataFrame(
            columns=["org", "repo_count", "total_stars", "total_forks", "total_open_issues"]
        )
    else:
        gh_agg = (
            github_df.groupby("org")
            .agg(
                repo_count=("full_name", "count"),
                total_stars=("stars", "sum"),
                total_forks=("forks", "sum"),
                total_open_issues=("open_issues", "sum"),
            )
            .reset_index()
        )

    if hn_df.empty:
        hn_agg = pd.DataFrame(columns=["org", "hn_mentions", "hn_points"])
    else:
        hn_agg = (
            hn_df.groupby("company")
            .agg(hn_mentions=("story_url", "count"), hn_points=("points", "sum"))
            .reset_index()
            .rename(columns={"company": "org"})
        )

    gold = gh_agg.merge(hn_agg, on="org", how="outer")
    numeric_cols = [c for c in gold.columns if c != "org"]
    gold[numeric_cols] = gold[numeric_cols].fillna(0).astype(int)
    gold["computed_at"] = pd.Timestamp.now(tz="UTC")
    return gold


def publish_gold(**_: object) -> None:
    """Build the per-company gold table and publish it to Delta gold + BigQuery.

    Both sinks are optional at runtime: Spark/Delta and the BigQuery client are
    imported lazily and a missing dependency or credentials downgrades to a
    skip+log rather than failing the DAG (same policy as the bronze write).
    """
    github_df = pd.concat(
        [postgres_writer.query_github_repos(org=o, limit=1000) for o in TRACKED_ORGS],
        ignore_index=True,
    )
    hn_df = pd.concat(
        [postgres_writer.query_hn_stories(company=o, limit=1000) for o in TRACKED_ORGS],
        ignore_index=True,
    )
    gold = build_company_signals(github_df, hn_df)
    print(f"[gold] built company_signals rows={len(gold)}")

    _write_gold_delta(gold, "company_signals")
    _publish_bigquery(gold, "company_signals")


def _write_gold_delta(pdf: pd.DataFrame, table: str) -> None:
    if pdf.empty:
        return
    try:
        from pyspark.sql import SparkSession

        from mosaic.storage.delta_writer import write_gold
    except ImportError as exc:  # pragma: no cover - depends on worker env
        print(f"[gold] skipping Delta write for {table}: pyspark unavailable ({exc})")
        return

    spark = (
        SparkSession.builder.appName("mosaic-batch-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    write_gold(spark.createDataFrame(pdf), f"{GOLD_ROOT}/{table}")
    print(f"[gold] wrote {len(pdf)} rows to {GOLD_ROOT}/{table}")


def _publish_bigquery(pdf: pd.DataFrame, table: str) -> None:
    if pdf.empty:
        return
    if not os.environ.get("BIGQUERY_PROJECT_ID"):
        print(f"[bigquery] skipping {table}: BIGQUERY_PROJECT_ID not set")
        return
    try:
        from mosaic.storage.bigquery_writer import write_dataframe
    except ImportError as exc:  # pragma: no cover - depends on worker env
        print(f"[bigquery] skipping {table}: client unavailable ({exc})")
        return
    write_dataframe(pdf, table_name=table, mode="overwrite")
    print(f"[bigquery] published {len(pdf)} rows to {table}")


with DAG(
    dag_id="mosaic_daily_refresh",
    default_args=DEFAULT_ARGS,
    description="Daily refresh of company intelligence signals.",
    schedule="0 2 * * *",
    start_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["mosaic", "batch"],
) as dag:
    github_task = PythonOperator(
        task_id="refresh_github",
        python_callable=refresh_github_for_all_orgs,
    )

    hn_task = PythonOperator(
        task_id="refresh_hn",
        python_callable=refresh_hn_mentions,
    )

    dq_task = PythonOperator(
        task_id="run_dq_checks",
        python_callable=run_dq_checks,
    )

    publish_task = PythonOperator(
        task_id="publish_gold",
        python_callable=publish_gold,
    )

    github_task >> hn_task >> dq_task >> publish_task
