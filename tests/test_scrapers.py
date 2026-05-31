"""Smoke tests for scrapers (mock network, no live calls)."""

from unittest.mock import Mock, patch

import pytest

from mosaic.scrapers.github_api import scrape_github_org
from mosaic.scrapers.hackernews_api import scrape_hn_company_mentions
from mosaic.scrapers import jobs_scraper, reddit_api


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


# --- Reddit ---------------------------------------------------------------


@patch("mosaic.scrapers.reddit_api.requests.post")
def test_reddit_get_access_token(mock_post, monkeypatch):
    monkeypatch.setenv("REDDIT_CLIENT_ID", "id")
    monkeypatch.setenv("REDDIT_CLIENT_SECRET", "secret")
    mock_post.return_value = Mock(
        status_code=200, json=lambda: {"access_token": "tok123"}
    )
    mock_post.return_value.raise_for_status = Mock()

    token = reddit_api.get_access_token()
    assert token == "tok123"
    # client-credentials grant sent
    assert mock_post.call_args.kwargs["data"]["grant_type"] == "client_credentials"
    assert mock_post.call_args.kwargs["auth"] == ("id", "secret")


def test_reddit_get_access_token_requires_creds(monkeypatch):
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    with pytest.raises(RuntimeError):
        reddit_api.get_access_token()


@patch("mosaic.scrapers.reddit_api.requests.get")
def test_scrape_reddit_mentions_parses_response(mock_get):
    mock_get.return_value = Mock(
        status_code=200,
        json=lambda: {
            "data": {
                "children": [
                    {
                        "data": {
                            "subreddit": "dataengineering",
                            "title": "Databricks vs Snowflake",
                            "score": 42,
                            "num_comments": 17,
                            "author": "someuser",
                            "created_utc": 1714521600,
                            "permalink": "/r/dataengineering/comments/abc/",
                        }
                    }
                ]
            }
        },
    )
    mock_get.return_value.raise_for_status = Mock()

    posts = reddit_api.scrape_reddit_mentions(
        company="Databricks",
        subreddits=["dataengineering"],
        limit=5,
        token="tok123",  # bypass OAuth fetch
    )
    assert len(posts) == 1
    assert posts[0].subreddit == "dataengineering"
    assert posts[0].score == 42
    assert posts[0].num_comments == 17
    assert posts[0].author == "someuser"
    # Authorization header carries the supplied token
    assert mock_get.call_args.kwargs["headers"]["Authorization"] == "bearer tok123"


@patch("mosaic.scrapers.reddit_api.requests.get")
def test_scrape_reddit_mentions_multiple_subreddits(mock_get):
    mock_get.return_value = Mock(
        status_code=200, json=lambda: {"data": {"children": []}}
    )
    mock_get.return_value.raise_for_status = Mock()

    reddit_api.scrape_reddit_mentions(
        company="X", subreddits=["a", "b", "c"], token="t"
    )
    # one request per subreddit
    assert mock_get.call_count == 3


# --- Jobs (Playwright) ----------------------------------------------------


def test_jobs_scraper_imports_cleanly():
    # Module imports regardless of Playwright availability.
    assert hasattr(jobs_scraper, "scrape_company_jobs")
    assert hasattr(jobs_scraper, "JobListing")


def test_jobs_scraper_unknown_source_raises():
    if not jobs_scraper._PLAYWRIGHT_AVAILABLE:
        pytest.skip("Playwright not installed")
    import asyncio

    with pytest.raises(ValueError):
        asyncio.run(jobs_scraper.scrape_company_jobs("acme", source="nope"))


def test_jobs_scraper_missing_playwright_raises(monkeypatch):
    monkeypatch.setattr(jobs_scraper, "_PLAYWRIGHT_AVAILABLE", False)
    import asyncio

    with pytest.raises(RuntimeError, match="Playwright is not installed"):
        asyncio.run(jobs_scraper.scrape_company_jobs("acme"))


def test_job_listing_model_shape():
    listing = jobs_scraper.JobListing(
        company="acme",
        title="Staff Engineer",
        location="Remote",
        posted_at=None,
        source_url="https://jobs.lever.co/acme/123",
    )
    assert listing.company == "acme"
    assert listing.location == "Remote"
