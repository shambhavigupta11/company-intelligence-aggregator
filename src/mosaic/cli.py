"""CLI entry point: `mosaic <command>`."""

from datetime import timedelta

import click

from mosaic.data_quality.checks import freshness, null_rate, row_count_anomaly
from mosaic.data_quality.reporters import report_console
from mosaic.scrapers.github_api import scrape_github_org
from mosaic.scrapers.hackernews_api import scrape_hn_company_mentions
from mosaic.scrapers.wikipedia_api import fetch_company_facts
from mosaic.storage import postgres_writer


@click.group()
def main() -> None:
    """Mosaic CLI."""


@main.command()
@click.option("--org", required=True, help="GitHub org login, e.g. 'databricks'.")
@click.option("--limit", default=10, help="Max repos to fetch.")
@click.option("--write-postgres", is_flag=True, help="Persist results to Postgres.")
def github(org: str, limit: int, write_postgres: bool) -> None:
    """Fetch GitHub engineering signals for an org."""
    results = scrape_github_org(org=org, limit=limit)
    click.echo(f"Fetched {len(results)} repos for org='{org}'.")
    for repo in results:
        click.echo(
            f"  {repo.name:40s}  stars={repo.stars:>6}  language={repo.language or '-'}"
        )

    if write_postgres:
        postgres_writer.init_schema()
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
            for r in results
        ]
        n = postgres_writer.upsert_github_repos(rows)
        click.echo(f"Wrote {n} rows to Postgres (github_repos).")


@main.command()
@click.option("--company", required=True, help="Company name to search HN for.")
@click.option("--limit", default=10, help="Max stories to fetch.")
@click.option("--write-postgres", is_flag=True, help="Persist results to Postgres.")
def hn(company: str, limit: int, write_postgres: bool) -> None:
    """Fetch HackerNews mentions of a company."""
    results = scrape_hn_company_mentions(company=company, limit=limit)
    click.echo(f"Found {len(results)} HN stories mentioning '{company}'.")
    for story in results:
        click.echo(f"  [{story.points:>4}pt] {story.title}")

    if write_postgres:
        postgres_writer.init_schema()
        rows = [
            {
                "story_url": s.url or f"hn://{s.created_at.isoformat()}/{hash(s.title)}",
                "company": company,
                "title": s.title,
                "points": s.points,
                "num_comments": s.num_comments,
                "author": s.author,
                "created_at": s.created_at,
            }
            for s in results
        ]
        n = postgres_writer.upsert_hn_stories(rows)
        click.echo(f"Wrote {n} rows to Postgres (hn_stories).")


@main.command()
@click.option("--company", required=True, help="Company name to look up on Wikipedia.")
@click.option("--write-postgres", is_flag=True, help="Persist result to Postgres.")
def wiki(company: str, write_postgres: bool) -> None:
    """Fetch company facts from Wikipedia."""
    facts = fetch_company_facts(company)
    if not facts:
        click.echo(f"No Wikipedia page found for '{company}'.")
        return

    click.echo(f"{facts.title} ({facts.description or 'no description'})")
    if facts.summary:
        click.echo(facts.summary[:300] + ("..." if len(facts.summary) > 300 else ""))
    if facts.url:
        click.echo(f"  -> {facts.url}")

    if write_postgres:
        postgres_writer.init_schema()
        n = postgres_writer.upsert_wikipedia_company(
            {
                "name": facts.name,
                "title": facts.title,
                "summary": facts.summary,
                "description": facts.description,
                "url": facts.url,
                "thumbnail_url": facts.thumbnail_url,
            }
        )
        click.echo(f"Wrote {n} row to Postgres (wikipedia_companies).")


@main.command()
@click.option(
    "--companies",
    default="databricks,snowflake-labs,dbt-labs",
    help="Comma-separated list of companies to enrich.",
)
def demo(companies: str) -> None:
    """Run the full pipeline end-to-end: scrape → Postgres → DQ checks.

    Prerequisites: `docker compose up -d` (starts Postgres).
    """
    company_list = [c.strip() for c in companies.split(",") if c.strip()]
    click.echo(f"Demo: scraping {len(company_list)} companies → Postgres → DQ checks.\n")

    postgres_writer.init_schema()
    click.echo("✓ Postgres schema initialized.\n")

    all_github_rows: list[dict] = []
    for org in company_list:
        click.echo(f"[github] fetching org={org}…")
        repos = scrape_github_org(org=org, limit=10)
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
        postgres_writer.upsert_github_repos(rows)
        all_github_rows.extend(rows)
        click.echo(f"  wrote {len(rows)} repos.")

    for company in company_list:
        click.echo(f"[wiki] fetching {company}…")
        facts = fetch_company_facts(company)
        if facts:
            postgres_writer.upsert_wikipedia_company(
                {
                    "name": facts.name,
                    "title": facts.title,
                    "summary": facts.summary,
                    "description": facts.description,
                    "url": facts.url,
                    "thumbnail_url": facts.thumbnail_url,
                }
            )
            click.echo(f"  wrote 1 wiki row ({facts.title}).")

    click.echo("\n[dq] running data quality checks on github_repos…\n")
    import pandas as pd

    df = pd.DataFrame(all_github_rows)
    results = [
        row_count_anomaly(df, table="github_repos", baseline=[20, 25, 30, 28, 27]),
        freshness(df, table="github_repos", ts_column="pushed_at", max_age=timedelta(days=365)),
        *null_rate(df, table="github_repos", max_null_rate=0.2),
    ]
    report_console(results)

    failing = [r for r in results if not r.passed]
    if failing:
        rows = [
            {
                "check_name": r.check_name,
                "table": r.table,
                "passed": r.passed,
                "severity": r.severity,
                "message": r.message,
                "measured_at": r.measured_at,
            }
            for r in failing
        ]
        postgres_writer.insert_dq_alerts(rows)
        click.echo(f"\nLogged {len(rows)} failing DQ checks to Postgres (dq_alerts).")

    click.echo(
        "\nDone. Try:\n"
        "  - `python -m mosaic.api.app` to start the API\n"
        "  - Open http://localhost:5050/api/companies/databricks/github (reads from Postgres)"
    )


if __name__ == "__main__":
    main()
