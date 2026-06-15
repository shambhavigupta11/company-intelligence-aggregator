"""Spark Structured Streaming job: reads scraped events from Kafka, lands to Delta bronze.

Each source (GitHub, HackerNews, Reddit, jobs) has its own Kafka topic, schema,
and bronze Delta table, declared once in ``STREAM_SPECS``. ``run()`` starts one
streaming query per spec and blocks until any of them terminates.

Run locally with PySpark, or deploy to Databricks Free Edition.
"""

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BRONZE_ROOT = os.environ.get("BRONZE_PATH_ROOT", "./data/bronze")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "./data/checkpoints")


@dataclass(frozen=True)
class StreamSpec:
    """One Kafka source → Delta bronze table.

    ``watermark_col`` is the event-time column used for watermarking; set it to
    ``None`` for sources without a reliable event timestamp (the records still
    land, just without late-data handling).
    """

    name: str
    topic: str
    schema: StructType
    watermark_col: str | None
    watermark_delay: str = "10 minutes"

    @property
    def bronze_path(self) -> str:
        return f"{BRONZE_ROOT}/{self.name}"

    @property
    def checkpoint_path(self) -> str:
        return f"{CHECKPOINT_ROOT}/{self.name}"


# Matches the GitHubRepo scraper model published to the github.repos topic.
GITHUB_SCHEMA = StructType(
    [
        StructField("full_name", StringType()),
        StructField("name", StringType()),
        StructField("description", StringType()),
        StructField("language", StringType()),
        StructField("stars", IntegerType()),
        StructField("forks", IntegerType()),
        StructField("open_issues", IntegerType()),
        StructField("pushed_at", TimestampType()),
    ]
)

HN_SCHEMA = StructType(
    [
        StructField("title", StringType()),
        StructField("url", StringType()),
        StructField("points", IntegerType()),
        StructField("author", StringType()),
        StructField("created_at", TimestampType()),
        StructField("num_comments", IntegerType()),
    ]
)

REDDIT_SCHEMA = StructType(
    [
        StructField("subreddit", StringType()),
        StructField("title", StringType()),
        StructField("score", IntegerType()),
        StructField("num_comments", IntegerType()),
        StructField("author", StringType()),
        StructField("created_utc", TimestampType()),
        StructField("permalink", StringType()),
    ]
)

JOBS_SCHEMA = StructType(
    [
        StructField("company", StringType()),
        StructField("title", StringType()),
        StructField("location", StringType()),
        # Job boards expose posted_at as free text (e.g. "3 days ago"), so it
        # stays a string and this source is not watermarked.
        StructField("posted_at", StringType()),
        StructField("source_url", StringType()),
    ]
)

STREAM_SPECS: list[StreamSpec] = [
    StreamSpec(
        name="github_repos",
        topic="github.repos",
        schema=GITHUB_SCHEMA,
        watermark_col="pushed_at",
    ),
    StreamSpec(
        name="hn_stories",
        topic="hn.stories",
        schema=HN_SCHEMA,
        watermark_col="created_at",
    ),
    StreamSpec(
        name="reddit_posts",
        topic="reddit.posts",
        schema=REDDIT_SCHEMA,
        watermark_col="created_utc",
    ),
    StreamSpec(
        name="job_listings",
        topic="jobs.listings",
        schema=JOBS_SCHEMA,
        watermark_col=None,
    ),
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("mosaic-streaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def parse_stream(raw: DataFrame, spec: StreamSpec) -> DataFrame:
    """Parse the Kafka ``value`` payload as JSON for ``spec`` and stamp ingest time.

    Pure transformation (no I/O) so it can be unit-tested against a batch
    DataFrame as well as a streaming one.
    """
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), spec.schema).alias("event"))
        .select("event.*")
        .withColumn("ingested_at", current_timestamp())
    )
    if spec.watermark_col:
        parsed = parsed.withWatermark(spec.watermark_col, spec.watermark_delay)
    return parsed


def start_query(spark: SparkSession, spec: StreamSpec):
    """Wire one Kafka topic → parsed bronze Delta stream and start it."""
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", spec.topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    return (
        parse_stream(raw_stream, spec)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", spec.checkpoint_path)
        .start(spec.bronze_path)
    )


def run(specs: list[StreamSpec] | None = None) -> None:
    """Start a streaming query per source and block until any terminates."""
    specs = specs or STREAM_SPECS
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    for spec in specs:
        start_query(spark, spec)
        print(f"[stream] started {spec.topic} -> {spec.bronze_path}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
