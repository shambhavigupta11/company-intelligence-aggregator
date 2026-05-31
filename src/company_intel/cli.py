"""CLI entry point: `company-intel <command>`."""

import click

from company_intel.scrapers.github_api import scrape_github_org
from company_intel.scrapers.hackernews_api import scrape_hn_company_mentions


@click.group()
def main() -> None:
    """Company Intelligence Aggregator CLI."""


@main.command()
@click.option("--org", required=True, help="GitHub org login, e.g. 'databricks'.")
@click.option("--limit", default=10, help="Max repos to fetch.")
def github(org: str, limit: int) -> None:
    """Fetch GitHub engineering signals for an org."""
    results = scrape_github_org(org=org, limit=limit)
    click.echo(f"Fetched {len(results)} repos for org='{org}'.")
    for repo in results:
        click.echo(
            f"  {repo.name:40s}  stars={repo.stars:>6}  language={repo.language or '-'}"
        )


@main.command()
@click.option("--company", required=True, help="Company name to search HN for.")
@click.option("--limit", default=10, help="Max stories to fetch.")
def hn(company: str, limit: int) -> None:
    """Fetch HackerNews mentions of a company."""
    results = scrape_hn_company_mentions(company=company, limit=limit)
    click.echo(f"Found {len(results)} HN stories mentioning '{company}'.")
    for story in results:
        click.echo(f"  [{story.points:>4}pt] {story.title}")


if __name__ == "__main__":
    main()
