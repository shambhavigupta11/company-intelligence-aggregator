"""PostgreSQL writer — operational store for the Flask API to query.

The schema lives in `schema.sql`. `init_schema()` applies it idempotently
(safe to call on every startup).
"""

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_engine() -> Engine:
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "mosaic")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def init_schema(engine: Engine | None = None) -> None:
    """Apply schema.sql idempotently. Safe to call repeatedly."""
    eng = engine or get_engine()
    schema_sql = SCHEMA_PATH.read_text()
    with eng.begin() as conn:
        for statement in [s.strip() for s in schema_sql.split(";") if s.strip()]:
            conn.execute(text(statement))


def upsert_github_repos(rows: list[dict], engine: Engine | None = None) -> int:
    """Upsert GitHub repo rows keyed by full_name. Returns row count written."""
    if not rows:
        return 0
    eng = engine or get_engine()
    sql = text(
        """
        INSERT INTO github_repos
            (full_name, org, name, description, language, stars, forks, open_issues, pushed_at)
        VALUES
            (:full_name, :org, :name, :description, :language, :stars, :forks, :open_issues, :pushed_at)
        ON CONFLICT (full_name) DO UPDATE SET
            description = EXCLUDED.description,
            language    = EXCLUDED.language,
            stars       = EXCLUDED.stars,
            forks       = EXCLUDED.forks,
            open_issues = EXCLUDED.open_issues,
            pushed_at   = EXCLUDED.pushed_at,
            scraped_at  = NOW()
        """
    )
    with eng.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def upsert_hn_stories(rows: list[dict], engine: Engine | None = None) -> int:
    """Upsert HackerNews story rows keyed by story_url. Returns row count written."""
    if not rows:
        return 0
    eng = engine or get_engine()
    sql = text(
        """
        INSERT INTO hn_stories
            (story_url, company, title, points, num_comments, author, created_at)
        VALUES
            (:story_url, :company, :title, :points, :num_comments, :author, :created_at)
        ON CONFLICT (story_url) DO UPDATE SET
            points       = EXCLUDED.points,
            num_comments = EXCLUDED.num_comments,
            scraped_at   = NOW()
        """
    )
    with eng.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def upsert_wikipedia_company(row: dict, engine: Engine | None = None) -> int:
    """Upsert a single wikipedia company record."""
    eng = engine or get_engine()
    sql = text(
        """
        INSERT INTO wikipedia_companies
            (name, title, summary, description, url, thumbnail_url)
        VALUES
            (:name, :title, :summary, :description, :url, :thumbnail_url)
        ON CONFLICT (name) DO UPDATE SET
            title         = EXCLUDED.title,
            summary       = EXCLUDED.summary,
            description   = EXCLUDED.description,
            url           = EXCLUDED.url,
            thumbnail_url = EXCLUDED.thumbnail_url,
            scraped_at    = NOW()
        """
    )
    with eng.begin() as conn:
        conn.execute(sql, row)
    return 1


def insert_dq_alerts(rows: list[dict], engine: Engine | None = None) -> int:
    """Insert DQ check results into the alerts log."""
    if not rows:
        return 0
    eng = engine or get_engine()
    sql = text(
        """
        INSERT INTO dq_alerts (check_name, "table", passed, severity, message, measured_at)
        VALUES (:check_name, :table, :passed, :severity, :message, :measured_at)
        """
    )
    with eng.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def query_github_repos(org: str, limit: int = 10, engine: Engine | None = None) -> pd.DataFrame:
    """Read recent GitHub repos for an org from Postgres."""
    eng = engine or get_engine()
    sql = """
        SELECT full_name, org, name, description, language, stars, forks, open_issues,
               pushed_at, scraped_at
        FROM github_repos
        WHERE org = %(org)s
        ORDER BY pushed_at DESC
        LIMIT %(limit)s
    """
    return pd.read_sql(sql, eng, params={"org": org, "limit": limit})


def query_hn_stories(company: str, limit: int = 10, engine: Engine | None = None) -> pd.DataFrame:
    """Read recent HN stories for a company from Postgres."""
    eng = engine or get_engine()
    sql = """
        SELECT story_url, company, title, points, num_comments, author,
               created_at, scraped_at
        FROM hn_stories
        WHERE company = %(company)s
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """
    return pd.read_sql(sql, eng, params={"company": company, "limit": limit})


def query_recent_dq_failures(limit: int = 20, engine: Engine | None = None) -> pd.DataFrame:
    eng = engine or get_engine()
    sql = """
        SELECT check_name, "table", passed, severity, message, measured_at
        FROM dq_alerts
        WHERE NOT passed
        ORDER BY measured_at DESC
        LIMIT %(limit)s
    """
    return pd.read_sql(sql, eng, params={"limit": limit})
