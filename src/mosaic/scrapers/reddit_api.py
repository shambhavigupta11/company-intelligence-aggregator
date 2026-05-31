"""Reddit API client — subreddit mentions and sentiment proxies.

TODO Phase 2: implement OAuth flow using REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET
env vars, then search r/programming, r/dataengineering, r/india, etc.
"""

from datetime import datetime

from pydantic import BaseModel


class RedditPost(BaseModel):
    subreddit: str
    title: str
    score: int
    num_comments: int
    author: str
    created_utc: datetime
    permalink: str


def scrape_reddit_mentions(
    company: str, subreddits: list[str], limit: int = 25
) -> list[RedditPost]:
    """Search given subreddits for company mentions. (Stub — Phase 2)"""
    raise NotImplementedError("Reddit scraper lands in Phase 2.")
