"""HTTP routes for the company intelligence API."""

from flask import Flask, jsonify, request

from mosaic.scrapers.github_api import scrape_github_org
from mosaic.scrapers.hackernews_api import scrape_hn_company_mentions
from mosaic.storage import postgres_writer


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
        # DQ failures are mirrored from the dq.alerts Kafka topic into the
        # Postgres `dq_alerts` table; serve the most recent failures from there.
        limit = int(request.args.get("limit", 20))
        df = postgres_writer.query_recent_dq_failures(limit=limit)
        # Stringify timestamps so jsonify can serialize the records.
        if "measured_at" in df.columns:
            df["measured_at"] = df["measured_at"].astype(str)
        records = df.to_dict(orient="records")
        return jsonify(records), 200
