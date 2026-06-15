"""Delta Lake writer — medallion architecture: bronze, silver, gold."""

from pyspark.sql import DataFrame, SparkSession


def write_bronze(df: DataFrame, path: str, mode: str = "append") -> None:
    """Write raw records to bronze with no schema enforcement beyond what's incoming."""
    (
        df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .save(path)
    )


def write_silver(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    """Write cleaned + deduplicated records to silver."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def write_gold(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    """Write business-level aggregates to gold.

    Gold tables are the curated, query-ready layer that feeds the analytics
    store (BigQuery) and the serving API. Overwrites in place so each refresh
    publishes a consistent snapshot.
    """
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def optimize_table(spark: SparkSession, path: str, zorder_cols: list[str]) -> None:
    """OPTIMIZE + Z-ORDER on a Delta table for query performance."""
    cols = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
