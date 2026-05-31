"""BigQuery writer — analytics layer.

Free tier: 1TB query / month + 10GB storage. Tables land in dataset
configured via BIGQUERY_DATASET env var.
"""

import os

import pandas as pd
from google.cloud import bigquery


def get_client() -> bigquery.Client:
    project = os.environ["BIGQUERY_PROJECT_ID"]
    return bigquery.Client(project=project)


def write_dataframe(df: pd.DataFrame, table_name: str, mode: str = "append") -> None:
    """Load a pandas DataFrame to a BigQuery table."""
    client = get_client()
    dataset = os.environ["BIGQUERY_DATASET"]
    table_id = f"{client.project}.{dataset}.{table_name}"

    write_disposition = (
        bigquery.WriteDisposition.WRITE_APPEND
        if mode == "append"
        else bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
