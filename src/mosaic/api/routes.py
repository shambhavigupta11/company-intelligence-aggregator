"""HTTP routes for the company intelligence API."""

from flask import Flask, jsonify, request

from mosaic.scrapers.github_api import scrape_github_org
from mosaic.scrapers.hackernews_api import scrape_hn_company_mentions


def register_routes(app: Flask) -> None:
    @app.get("/health")
    def health() -> tuple:
        return jsonify({"status": "ok"}), 200

    @app.get("/api/companies/<org>/github")
    def github_signals(org: str) -> tuple:
        limit = int(request.args.get("limit", 10))
        repos = scrape_github_org(org=org, limit=limit)
        return jsonify([r.model_dump(mode="json") for r in repos]), 200

    @app.get("/api/companies/<company>/hn")
    def hn_mentions(company: str) -> tuple:
        limit = int(request.args.get("limit", 10))
        stories = scrape_hn_company_mentions(company=company, limit=limit)
        return jsonify([s.model_dump(mode="json") for s in stories]), 200

    @app.get("/api/alerts")
    def alerts() -> tuple:
        # TODO Phase 2: read from dq.alerts Kafka topic / Postgres mirror
        return jsonify([]), 200
