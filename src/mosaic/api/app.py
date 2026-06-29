"""Flask app entry point.

Run locally with ``python -m mosaic.api.app`` (Flask dev server) or in
production via ``gunicorn "mosaic.api.app:create_app()"``.
"""

import logging
import os

from flask import Flask
from flask_cors import CORS

from mosaic.api.routes import register_routes

logger = logging.getLogger(__name__)


def _maybe_init_schema() -> None:
    """Best-effort Postgres schema init on boot.

    The GitHub and HN endpoints scrape live and need no database, so a missing
    or unreachable Postgres must not stop the API from serving — it only
    disables the alerts endpoint. We attempt init only when a host is
    configured, and downgrade any failure to a warning.
    """
    if not os.environ.get("POSTGRES_HOST"):
        return
    try:
        from mosaic.storage import postgres_writer

        postgres_writer.init_schema()
        logger.info("Postgres schema initialized.")
    except Exception as exc:  # pragma: no cover - depends on live DB
        logger.warning("Skipping schema init (DB unavailable): %s", exc)


def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app)
    register_routes(app)
    _maybe_init_schema()
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", os.environ.get("FLASK_PORT", 5050)))
    create_app().run(host="0.0.0.0", port=port, debug=True)
