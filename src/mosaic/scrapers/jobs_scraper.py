"""Playwright-based scraper for public job boards.

Headless-browser scraping with rate limiting and politeness controls.
Targets public listing pages (Lever, Greenhouse), extracts role counts and
titles as headcount signals.

Playwright is an optional/heavy dependency. The import is gated so this module
imports cleanly even when Playwright is not installed; the actual scrape raises
a clear error if it is missing.

Respect robots.txt and ToS for any target site.
"""

import asyncio

from pydantic import BaseModel

try:  # pragma: no cover - exercised only when Playwright is installed
    from playwright.async_api import async_playwright

    _PLAYWRIGHT_AVAILABLE = True
except ImportError:  # pragma: no cover
    async_playwright = None  # type: ignore[assignment]
    _PLAYWRIGHT_AVAILABLE = False

# Per-board public listing URL templates and the CSS selectors that mark a
# single job posting on each board.
JOB_BOARDS: dict[str, dict[str, str]] = {
    "lever": {
        "url": "https://jobs.lever.co/{slug}",
        "posting": "div.posting",
        "title": "h5",
        "location": ".posting-categories .location",
        "link": "a.posting-title",
    },
    "greenhouse": {
        "url": "https://boards.greenhouse.io/{slug}",
        "posting": "div.opening",
        "title": "a",
        "location": ".location",
        "link": "a",
    },
}

# Politeness: minimum delay (seconds) between page navigations.
DEFAULT_RATE_LIMIT_SECONDS = 2.0


class JobListing(BaseModel):
    company: str
    title: str
    location: str | None
    posted_at: str | None
    source_url: str


async def scrape_company_jobs(
    company_slug: str,
    source: str = "lever",
    rate_limit_seconds: float = DEFAULT_RATE_LIMIT_SECONDS,
) -> list[JobListing]:
    """Headless-browser scrape of a company's public jobs board.

    Args:
        company_slug: the company's board slug (e.g. "databricks").
        source: which board to scrape — one of ``JOB_BOARDS``.
        rate_limit_seconds: minimum delay applied before navigating, for politeness.

    Returns a list of :class:`JobListing` records (open-role headcount signal).
    """
    if not _PLAYWRIGHT_AVAILABLE:
        raise RuntimeError(
            "Playwright is not installed. Install it with `pip install playwright` "
            "and `playwright install chromium` to use the jobs scraper."
        )

    if source not in JOB_BOARDS:
        raise ValueError(f"Unknown jobs source '{source}'. Known: {sorted(JOB_BOARDS)}")

    board = JOB_BOARDS[source]
    url = board["url"].format(slug=company_slug)

    listings: list[JobListing] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            # Politeness delay before hitting the target site.
            await asyncio.sleep(rate_limit_seconds)
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

            postings = await page.query_selector_all(board["posting"])
            for posting in postings:
                title_el = await posting.query_selector(board["title"])
                location_el = await posting.query_selector(board["location"])
                link_el = await posting.query_selector(board["link"])

                title = (await title_el.inner_text()).strip() if title_el else None
                if not title:
                    continue
                location = (
                    (await location_el.inner_text()).strip() if location_el else None
                )
                href = (
                    await link_el.get_attribute("href") if link_el else None
                ) or url

                listings.append(
                    JobListing(
                        company=company_slug,
                        title=title,
                        location=location,
                        posted_at=None,
                        source_url=href,
                    )
                )
        finally:
            await browser.close()

    return listings


def scrape_company_jobs_sync(
    company_slug: str,
    source: str = "lever",
    rate_limit_seconds: float = DEFAULT_RATE_LIMIT_SECONDS,
) -> list[JobListing]:
    """Synchronous convenience wrapper around :func:`scrape_company_jobs`."""
    return asyncio.run(
        scrape_company_jobs(
            company_slug, source=source, rate_limit_seconds=rate_limit_seconds
        )
    )
