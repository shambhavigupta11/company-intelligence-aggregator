"""Reddit API client — subreddit mentions and sentiment proxies.

Uses Reddit's OAuth2 "client credentials" (application-only) flow with the
REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET / REDDIT_USER_AGENT env vars
(see .env.example), then searches the given subreddits for company mentions.
"""

import os
from datetime import datetime, timezone

import requests
from pydantic import BaseModel

REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_OAUTH_API = "https://oauth.reddit.com"
DEFAULT_USER_AGENT = "mosaic/0.1"


class RedditPost(BaseModel):
    subreddit: str
    title: str
    score: int
    num_comments: int
    author: str
    created_utc: datetime
    permalink: str


def _user_agent() -> str:
    return os.environ.get("REDDIT_USER_AGENT") or DEFAULT_USER_AGENT


def get_access_token() -> str:
    """Obtain an application-only OAuth token via the client-credentials flow.

    Requires REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in the environment.
    """
    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET must be set to use the Reddit scraper."
        )

    resp = requests.post(
        REDDIT_TOKEN_URL,
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials"},
        headers={"User-Agent": _user_agent()},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _parse_post(child: dict, subreddit: str) -> RedditPost:
    data = child.get("data", {})
    return RedditPost(
        subreddit=data.get("subreddit") or subreddit,
        title=data.get("title") or "(no title)",
        score=data.get("score") or 0,
        num_comments=data.get("num_comments") or 0,
        author=data.get("author") or "[deleted]",
        created_utc=datetime.fromtimestamp(data.get("created_utc") or 0, tz=timezone.utc),
        permalink=data.get("permalink") or "",
    )


def scrape_reddit_mentions(
    company: str,
    subreddits: list[str],
    limit: int = 25,
    token: str | None = None,
) -> list[RedditPost]:
    """Search the given subreddits for posts mentioning a company name.

    One restricted search request is issued per subreddit. Pass ``token`` to
    reuse an existing access token; otherwise one is fetched automatically.
    """
    access_token = token or get_access_token()
    headers = {
        "Authorization": f"bearer {access_token}",
        "User-Agent": _user_agent(),
    }

    posts: list[RedditPost] = []
    for subreddit in subreddits:
        url = f"{REDDIT_OAUTH_API}/r/{subreddit}/search"
        params = {
            "q": company,
            "restrict_sr": 1,
            "sort": "new",
            "limit": limit,
        }
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        children = resp.json().get("data", {}).get("children", [])
        posts.extend(_parse_post(child, subreddit) for child in children)

    return posts
