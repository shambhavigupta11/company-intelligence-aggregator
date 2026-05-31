"""GitHub API client — engineering activity signals for orgs and repos.

Unauthenticated: 60 req/hr. With GITHUB_TOKEN env var: 5000 req/hr.
"""

import os
from datetime import datetime
from typing import Any

import requests
from pydantic import BaseModel, Field

GITHUB_API = "https://api.github.com"


class GitHubRepo(BaseModel):
    name: str
    full_name: str
    stars: int = Field(alias="stargazers_count")
    forks: int = Field(alias="forks_count")
    open_issues: int = Field(alias="open_issues_count")
    language: str | None
    pushed_at: datetime
    description: str | None = None

    model_config = {"populate_by_name": True}


def _auth_headers() -> dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


def scrape_github_org(org: str, limit: int = 10) -> list[GitHubRepo]:
    """Fetch top public repos for a GitHub org, sorted by recent activity."""
    url = f"{GITHUB_API}/orgs/{org}/repos"
    params: dict[str, Any] = {"sort": "pushed", "per_page": limit}
    resp = requests.get(url, params=params, headers=_auth_headers(), timeout=30)
    resp.raise_for_status()
    return [GitHubRepo(**item) for item in resp.json()]


def scrape_repo_contributors(owner: str, repo: str, limit: int = 30) -> list[dict]:
    """Fetch contributor list with commit counts."""
    url = f"{GITHUB_API}/repos/{owner}/{repo}/contributors"
    params = {"per_page": limit}
    resp = requests.get(url, params=params, headers=_auth_headers(), timeout=30)
    resp.raise_for_status()
    return [
        {
            "login": c["login"],
            "contributions": c["contributions"],
            "avatar_url": c["avatar_url"],
        }
        for c in resp.json()
    ]
