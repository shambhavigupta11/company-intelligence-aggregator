-- Schema for the operational Postgres store.
-- Run via: psql -h localhost -U postgres -d mosaic -f schema.sql
-- Or programmatically via mosaic.storage.postgres_writer.init_schema()

CREATE TABLE IF NOT EXISTS github_repos (
    full_name        TEXT PRIMARY KEY,
    org              TEXT NOT NULL,
    name             TEXT NOT NULL,
    description      TEXT,
    language         TEXT,
    stars            INTEGER NOT NULL DEFAULT 0,
    forks            INTEGER NOT NULL DEFAULT 0,
    open_issues      INTEGER NOT NULL DEFAULT 0,
    pushed_at        TIMESTAMPTZ NOT NULL,
    scraped_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_github_repos_org ON github_repos(org);
CREATE INDEX IF NOT EXISTS idx_github_repos_stars ON github_repos(stars DESC);

CREATE TABLE IF NOT EXISTS hn_stories (
    story_url        TEXT PRIMARY KEY,
    company          TEXT NOT NULL,
    title            TEXT NOT NULL,
    points           INTEGER NOT NULL DEFAULT 0,
    num_comments     INTEGER NOT NULL DEFAULT 0,
    author           TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    scraped_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hn_stories_company ON hn_stories(company);
CREATE INDEX IF NOT EXISTS idx_hn_stories_created_at ON hn_stories(created_at DESC);

CREATE TABLE IF NOT EXISTS wikipedia_companies (
    name             TEXT PRIMARY KEY,
    title            TEXT NOT NULL,
    summary          TEXT,
    description      TEXT,
    url              TEXT,
    thumbnail_url    TEXT,
    scraped_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dq_alerts (
    id               SERIAL PRIMARY KEY,
    check_name       TEXT NOT NULL,
    "table"          TEXT NOT NULL,
    passed           BOOLEAN NOT NULL,
    severity         TEXT NOT NULL,
    message          TEXT NOT NULL,
    measured_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_alerts_measured_at ON dq_alerts(measured_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_alerts_failed ON dq_alerts(measured_at DESC) WHERE NOT passed;
