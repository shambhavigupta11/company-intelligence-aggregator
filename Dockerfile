# Lean image for the Mosaic Flask API. Installs only the API runtime deps
# (requirements-api.txt) — not the full data-engineering stack — and serves
# the app factory with gunicorn.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PORT=5050

WORKDIR /app

# Install deps first so they cache across source changes.
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY src/ ./src/

EXPOSE 5050

# Exec form with `exec` so $PORT expands AND gunicorn replaces the shell as
# PID 1, receiving SIGTERM directly for graceful shutdown. 2 workers fit a
# small free-tier instance; the API is I/O-bound (scraping + DB reads).
CMD ["sh", "-c", "exec gunicorn 'mosaic.api.app:create_app()' --bind 0.0.0.0:${PORT} --workers 2 --timeout 60 --access-logfile -"]
