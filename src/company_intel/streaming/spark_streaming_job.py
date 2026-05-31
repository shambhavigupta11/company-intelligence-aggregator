"""Spark Structured Streaming job: reads from Kafka, lands to Delta bronze.

Run locally with PySpark, or deploy to Databricks Free Edition.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BRONZE_PATH = os.environ.get("BRONZE_PATH", "./data/bronze/github_events")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "./data/checkpoints/github_events")

GITHUB_EVENT_SCHEMA = StructType(
    [
        StructField("repo_full_name", StringType()),
        StructField("event_type", StringType()),
        StructField("actor", StringType()),
        StructField("created_at", TimestampType()),
        StructField("stars", IntegerType()),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("company-intel-streaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def run() -> None:
    """Stream Kafka → parse JSON → land to Delta bronze with watermarking."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "github.events")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), GITHUB_EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("ingested_at", current_timestamp())
        .withWatermark("created_at", "10 minutes")
    )

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(BRONZE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
