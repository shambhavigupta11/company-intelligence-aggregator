"""Airflow DAG: daily company intelligence refresh.

Scheduled to run nightly. Refreshes GitHub org metrics, HN mentions,
Wikipedia metadata for the tracked-companies list, then runs DQ checks
and publishes to BigQuery silver/gold layers.
"""

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

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

    github_task >> hn_task
