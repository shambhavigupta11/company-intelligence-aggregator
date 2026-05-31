"""Playwright-based scraper for public job boards.

TODO Phase 2: implement headless browser scraping with rate limiting and
politeness controls. Targets public listing pages, extracts role counts
and titles as headcount signals.

Respect robots.txt and ToS for any target site.
"""

from pydantic import BaseModel


class JobListing(BaseModel):
    company: str
    title: str
    location: str | None
    posted_at: str | None
    source_url: str


async def scrape_company_jobs(
    company_slug: str, source: str = "lever"
) -> list[JobListing]:
    """Headless-browser scrape of a company's public jobs board. (Stub — Phase 2)"""
    raise NotImplementedError("Jobs scraper lands in Phase 2.")
