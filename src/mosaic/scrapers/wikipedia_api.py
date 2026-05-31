"""Wikipedia REST API client — company metadata enrichment.

Uses Wikipedia's public REST API (no auth required, no rate limits for reasonable use).
"""

import requests
from pydantic import BaseModel

WIKIPEDIA_REST_API = "https://en.wikipedia.org/api/rest_v1/page/summary"
WIKIPEDIA_SEARCH_API = "https://en.wikipedia.org/w/api.php"


class CompanyFacts(BaseModel):
    name: str
    title: str
    summary: str | None = None
    description: str | None = None
    url: str | None = None
    thumbnail_url: str | None = None


def _search_wikipedia(query: str) -> str | None:
    """Use Wikipedia search to resolve a company name to a canonical page title."""
    params = {
        "action": "query",
        "list": "search",
        "srsearch": query,
        "srlimit": 1,
        "format": "json",
    }
    resp = requests.get(WIKIPEDIA_SEARCH_API, params=params, timeout=30)
    resp.raise_for_status()
    hits = resp.json().get("query", {}).get("search", [])
    return hits[0]["title"] if hits else None


def fetch_company_facts(company: str) -> CompanyFacts | None:
    """Resolve a company name to its Wikipedia summary."""
    title = _search_wikipedia(company)
    if not title:
        return None

    url = f"{WIKIPEDIA_REST_API}/{title.replace(' ', '_')}"
    resp = requests.get(url, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json()

    return CompanyFacts(
        name=company,
        title=data.get("title", title),
        summary=data.get("extract"),
        description=data.get("description"),
        url=(data.get("content_urls") or {}).get("desktop", {}).get("page"),
        thumbnail_url=(data.get("thumbnail") or {}).get("source"),
    )
