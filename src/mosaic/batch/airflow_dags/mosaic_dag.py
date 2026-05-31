"""Airflow DAG: daily company intelligence refresh.

Scheduled to run nightly. Refreshes GitHub org metrics, HN mentions,
Wikipedia metadata for the tracked-companies list, then runs DQ checks
and publishes to BigQuery silver/gold layers.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from mosaic.scrapers.github_api import scrape_github_org
from mosaic.scrapers.hackernews_api import scrape_hn_company_mentions

DEFAULT_ARGS = {
    "owner": "mosaic",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

TRACKED_ORGS = ["databricks", "snowflake-labs", "apache", "dbt-labs"]


def refresh_github_for_all_orgs(**_: object) -> None:
    for org in TRACKED_ORGS:
        repos = scrape_github_org(org=org, limit=30)
        # TODO Phase 2: write to bronze Delta + Postgres
        print(f"[github] org={org} repos={len(repos)}")


def refresh_hn_mentions(**_: object) -> None:
    for org in TRACKED_ORGS:
        stories = scrape_hn_company_mentions(company=org, limit=20)
        # TODO Phase 2: write to bronze Delta
        print(f"[hn] org={org} stories={len(stories)}")


with DAG(
    dag_id="mosaic_daily_refresh",
    default_args=DEFAULT_ARGS,
    description="Daily refresh of company intelligence signals.",
    schedule="0 2 * * *",
    start_date=datetime(2026, 5, 1),
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
