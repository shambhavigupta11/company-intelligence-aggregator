"""Flask app entry point."""

import os

from flask import Flask
from flask_cors import CORS

from company_intel.api.routes import register_routes


def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app)
    register_routes(app)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("FLASK_PORT", 5050))
    create_app().run(host="0.0.0.0", port=port, debug=True)
