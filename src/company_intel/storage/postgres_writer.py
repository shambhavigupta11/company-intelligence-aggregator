"""PostgreSQL writer — operational store for the Flask API to query."""

import os

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "company_intel")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def upsert_dataframe(df: pd.DataFrame, table_name: str, key_cols: list[str]) -> None:
    """Upsert a DataFrame into a Postgres table using staging + ON CONFLICT.

    Phase 1: simple replace. Phase 2: real upsert with key_cols.
    """
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists="replace", index=False)
