"""HackerNews Algolia API — search for company mentions."""

from datetime import datetime

import requests
from pydantic import BaseModel

HN_ALGOLIA_API = "https://hn.algolia.com/api/v1/search"


class HNStory(BaseModel):
    title: str
    url: str | None = None
    points: int = 0
    author: str
    created_at: datetime
    num_comments: int = 0


def scrape_hn_company_mentions(company: str, limit: int = 10) -> list[HNStory]:
    """Search HN for stories mentioning a company name."""
    params = {
        "query": company,
        "tags": "story",
        "hitsPerPage": limit,
    }
    resp = requests.get(HN_ALGOLIA_API, params=params, timeout=30)
    resp.raise_for_status()
    hits = resp.json().get("hits", [])
    return [
        HNStory(
            title=h.get("title") or h.get("story_title") or "(no title)",
            url=h.get("url"),
            points=h.get("points") or 0,
            author=h.get("author") or "anonymous",
            created_at=h["created_at"],
            num_comments=h.get("num_comments") or 0,
        )
        for h in hits
    ]
