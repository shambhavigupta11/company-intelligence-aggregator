"""Wikipedia / Wikidata API client — company metadata enrichment.

TODO Phase 2: implement using Wikipedia REST API and Wikidata SPARQL endpoint
for structured company facts (founders, founding date, HQ, industry).
"""

from pydantic import BaseModel


class CompanyFacts(BaseModel):
    name: str
    summary: str | None = None
    founders: list[str] = []
    founded_year: int | None = None
    headquarters: str | None = None
    industry: str | None = None


def fetch_company_facts(company: str) -> CompanyFacts:
    """Fetch structured facts about a company from Wikipedia + Wikidata. (Stub)"""
    raise NotImplementedError("Wikipedia enrichment lands in Phase 2.")
