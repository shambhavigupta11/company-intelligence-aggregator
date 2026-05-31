"""Smoke tests for scrapers (mock network, no live calls)."""

from unittest.mock import Mock, patch

from mosaic.scrapers.github_api import scrape_github_org
from mosaic.scrapers.hackernews_api import scrape_hn_company_mentions


@patch("mosaic.scrapers.github_api.requests.get")
def test_scrape_github_org_parses_response(mock_get):
    mock_get.return_value = Mock(
        status_code=200,
        json=lambda: [
            {
                "name": "delta",
                "full_name": "databricks/delta",
                "stargazers_count": 7500,
                "forks_count": 1600,
                "open_issues_count": 200,
                "language": "Scala",
                "pushed_at": "2026-05-01T12:00:00Z",
                "description": "Lakehouse storage layer",
            }
        ],
    )
    mock_get.return_value.raise_for_status = Mock()

    repos = scrape_github_org(org="databricks", limit=1)
    assert len(repos) == 1
    assert repos[0].name == "delta"
    assert repos[0].stars == 7500
    assert repos[0].language == "Scala"


@patch("mosaic.scrapers.hackernews_api.requests.get")
def test_scrape_hn_company_mentions_parses_response(mock_get):
    mock_get.return_value = Mock(
        status_code=200,
        json=lambda: {
            "hits": [
                {
                    "title": "Databricks announces new pricing",
                    "url": "https://example.com",
                    "points": 234,
                    "author": "pg",
                    "created_at": "2026-04-15T08:00:00Z",
                    "num_comments": 56,
                }
            ]
        },
    )
    mock_get.return_value.raise_for_status = Mock()

    stories = scrape_hn_company_mentions(company="databricks", limit=1)
    assert len(stories) == 1
    assert stories[0].points == 234
    assert "Databricks" in stories[0].title
