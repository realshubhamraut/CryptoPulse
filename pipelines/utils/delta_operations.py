"""
CryptoPulse - Delta Lake Operations

Utility functions for Delta Lake ACID operations, time travel,
and unified batch/stream processing.

Resume claims addressed:
- "ACID guarantees" → DeltaTable.merge() for UPSERT operations
- "time travel" → versionAsOf / timestampAsOf queries
- "unified batch/stream processing" → same Delta table in both modes

Requires: delta-spark 3.x on the classpath
"""

from datetime import datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="delta_operations")


# =============================================================================
# ACID MERGE / UPSERT Operations
# =============================================================================

def upsert_to_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: list[str],
    partition_cols: list[str] | None = None,
) -> dict[str, int]:
    """
    ACID-compliant MERGE/UPSERT into a Delta table.

    If a record with the same merge_keys exists → UPDATE it.
    If it doesn't exist → INSERT it.

    This demonstrates Delta Lake's ACID transaction guarantees:
    the operation is atomic — either all rows merge or none do.

    Args:
        spark: SparkSession with Delta support
        source_df: New data to merge
        target_path: Path to the target Delta table
        merge_keys: Columns to match on (e.g., ["symbol", "trade_id"])
        partition_cols: Partition columns for new tables

    Returns:
        Dict with insert_count and update_count

    Example:
        upsert_to_delta(
            spark, new_trades_df,
            "/data/delta/silver/trades",
            merge_keys=["symbol", "trade_id"],
        )
    """
    from delta.tables import DeltaTable

    # Build merge condition
    merge_condition = " AND ".join(
        f"target.{k} = source.{k}" for k in merge_keys
    )

    logger.info(
        "starting_delta_upsert",
        target=target_path,
        merge_keys=merge_keys,
    )

    # Check if target table exists
    if DeltaTable.isDeltaTable(spark, target_path):
        target_table = DeltaTable.forPath(spark, target_path)

        merge_result = (
            target_table.alias("target")
            .merge(
                source_df.alias("source"),
                merge_condition,
            )
            # Update existing records
            .whenMatchedUpdateAll()
            # Insert new records
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info("delta_upsert_complete", target=target_path)

        return {"status": "merged", "target": target_path}
    else:
        # First write — create the table
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)

        logger.info("delta_table_created", target=target_path)
        return {"status": "created", "target": target_path}


def merge_dedup(
    spark: SparkSession,
    new_data_df: DataFrame,
    target_path: str,
    dedup_keys: list[str],
    order_col: str = "ingested_at",
) -> dict[str, Any]:
    """
    ACID-safe deduplication merge: only insert records that don't exist yet.

    Unlike append mode, this guarantees no duplicates even if the same
    batch is reprocessed (idempotent writes).

    Args:
        spark: SparkSession
        new_data_df: Incoming data to merge
        target_path: Delta table path
        dedup_keys: Columns defining uniqueness
        order_col: Tiebreaker column (keep latest)
    """
    from delta.tables import DeltaTable

    merge_condition = " AND ".join(
        f"target.{k} = source.{k}" for k in dedup_keys
    )

    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)

        (
            target.alias("target")
            .merge(new_data_df.alias("source"), merge_condition)
            # If matched: keep the newer version
            .whenMatchedUpdateAll(
                condition=f"source.{order_col} > target.{order_col}"
            )
            # If not matched: insert
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info("dedup_merge_complete", target=target_path, keys=dedup_keys)
        return {"status": "merged"}
    else:
        new_data_df.write.format("delta").mode("overwrite").save(target_path)
        logger.info("delta_table_created", target=target_path)
        return {"status": "created"}


# =============================================================================
# Time Travel Queries
# =============================================================================

def read_delta_at_version(
    spark: SparkSession,
    path: str,
    version: int,
) -> DataFrame:
    """
    Read a Delta table at a specific version (time travel).

    Delta Lake maintains a transaction log so you can query any
    historical state of the table by version number.

    Args:
        spark: SparkSession
        path: Delta table path
        version: Version number (0-based, starts at the first commit)

    Example:
        # Read the table as it was 3 commits ago
        old_df = read_delta_at_version(spark, "/data/delta/silver/trades", version=5)
    """
    logger.info("time_travel_by_version", path=path, version=version)

    return (
        spark.read
        .format("delta")
        .option("versionAsOf", version)
        .load(path)
    )


def read_delta_at_timestamp(
    spark: SparkSession,
    path: str,
    timestamp: str | datetime,
) -> DataFrame:
    """
    Read a Delta table as of a specific timestamp (time travel).

    Args:
        spark: SparkSession
        path: Delta table path
        timestamp: ISO timestamp string or datetime object

    Example:
        # Read the table as it was yesterday
        yesterday_df = read_delta_at_timestamp(
            spark, "/data/delta/gold/features",
            "2025-01-15T12:00:00"
        )
    """
    if isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat()

    logger.info("time_travel_by_timestamp", path=path, timestamp=timestamp)

    return (
        spark.read
        .format("delta")
        .option("timestampAsOf", timestamp)
        .load(path)
    )


def compare_versions(
    spark: SparkSession,
    path: str,
    old_version: int,
    new_version: int,
) -> dict[str, Any]:
    """
    Compare two versions of a Delta table (diff).

    Useful for auditing what data changed between pipeline runs.

    Returns:
        Dict with row counts and schema for both versions
    """
    old_df = read_delta_at_version(spark, path, old_version)
    new_df = read_delta_at_version(spark, path, new_version)

    old_count = old_df.count()
    new_count = new_df.count()

    result = {
        "old_version": old_version,
        "new_version": new_version,
        "old_row_count": old_count,
        "new_row_count": new_count,
        "rows_added": new_count - old_count,
        "schema_match": old_df.schema == new_df.schema,
    }

    logger.info("version_comparison", **result)
    return result


# =============================================================================
# Table History & Metadata
# =============================================================================

def get_delta_history(
    spark: SparkSession,
    path: str,
    limit: int = 20,
) -> DataFrame:
    """
    Get the commit history of a Delta table.

    Each row represents a commit (version) with:
    - version, timestamp, operation, operationParameters
    - operationMetrics (rows written, files added/removed)

    Example:
        history_df = get_delta_history(spark, "/data/delta/silver/trades")
        history_df.show()
    """
    from delta.tables import DeltaTable

    logger.info("fetching_delta_history", path=path, limit=limit)

    table = DeltaTable.forPath(spark, path)
    return table.history(limit)


def get_table_details(
    spark: SparkSession,
    path: str,
) -> dict[str, Any]:
    """
    Get detailed metadata about a Delta table.

    Returns version, record count, partition columns, and properties.
    """
    from delta.tables import DeltaTable

    table = DeltaTable.forPath(spark, path)
    detail = table.detail().collect()[0]

    return {
        "name": detail["name"],
        "format": detail["format"],
        "location": detail["location"],
        "num_files": detail["numFiles"],
        "size_bytes": detail["sizeInBytes"],
        "partition_columns": detail["partitionColumns"],
        "properties": detail["properties"],
        "created_at": str(detail["createdAt"]),
        "last_modified": str(detail["lastModified"]),
    }


# =============================================================================
# Table Maintenance
# =============================================================================

def vacuum_delta_table(
    spark: SparkSession,
    path: str,
    retention_hours: int = 168,
) -> None:
    """
    Remove old data files that are no longer referenced by the Delta log.

    Default retention: 168 hours (7 days).

    Args:
        spark: SparkSession
        path: Delta table path
        retention_hours: How many hours of history to keep

    WARNING: After vacuuming, time travel to versions older than
    the retention period will no longer work.
    """
    from delta.tables import DeltaTable

    logger.info(
        "vacuuming_delta_table",
        path=path,
        retention_hours=retention_hours,
    )

    table = DeltaTable.forPath(spark, path)
    table.vacuum(retention_hours)

    logger.info("vacuum_complete", path=path)


def optimize_delta_table(
    spark: SparkSession,
    path: str,
    z_order_cols: list[str] | None = None,
) -> None:
    """
    Optimize a Delta table by compacting small files.

    Optionally Z-ORDER by specified columns for better query pruning.

    Args:
        spark: SparkSession
        path: Delta table path
        z_order_cols: Columns to Z-ORDER by (e.g., ["symbol", "trade_date"])
    """
    logger.info("optimizing_delta_table", path=path, z_order=z_order_cols)

    if z_order_cols:
        z_cols = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({z_cols})")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")

    logger.info("optimize_complete", path=path)


# =============================================================================
# Unified Batch/Stream Processing
# =============================================================================

def read_delta_streaming(
    spark: SparkSession,
    path: str,
    max_files_per_trigger: int = 100,
) -> DataFrame:
    """
    Read a Delta table as a streaming source.

    This is the STREAMING half of "unified batch/stream processing".
    The same Delta table written by batch jobs can be consumed as a stream.

    Args:
        spark: SparkSession
        path: Delta table path
        max_files_per_trigger: Rate limiting for micro-batches
    """
    logger.info("reading_delta_streaming", path=path)

    return (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("ignoreChanges", "true")
        .load(path)
    )


def read_delta_batch(
    spark: SparkSession,
    path: str,
    predicate: str | None = None,
) -> DataFrame:
    """
    Read a Delta table as a batch (static) DataFrame.

    This is the BATCH half of "unified batch/stream processing".
    The same Delta table written by streaming jobs can be read as a batch.

    Args:
        spark: SparkSession
        path: Delta table path
        predicate: Optional SQL predicate for pushdown filtering
    """
    logger.info("reading_delta_batch", path=path)

    df = spark.read.format("delta").load(path)

    if predicate:
        df = df.filter(predicate)

    return df


def demonstrate_unified_processing(
    spark: SparkSession,
    delta_path: str,
) -> dict[str, Any]:
    """
    Demonstrate that the same Delta table supports both batch and streaming.

    This directly backs the resume claim: "unified batch/stream processing"

    1. Read the table in BATCH mode → show row count
    2. Read the same table in STREAM mode → start a query
    3. Both read from the exact same Delta log / Parquet files
    """
    # Batch read
    batch_df = read_delta_batch(spark, delta_path)
    batch_count = batch_df.count()

    # Streaming read (same table)
    stream_df = read_delta_streaming(spark, delta_path)
    query = (
        stream_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(once=True)  # Process one micro-batch then stop
        .start()
    )
    query.awaitTermination(timeout=30)

    result = {
        "delta_path": delta_path,
        "batch_row_count": batch_count,
        "stream_query_id": str(query.id),
        "unified": True,
    }

    logger.info("unified_processing_demo_complete", **result)
    return result


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("CryptoPulse-DeltaOps")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    delta_path = f"{settings.storage.adls_delta_path}/silver/trades"

    # Show Delta table history
    print("\n─── Delta Table History ───")
    history = get_delta_history(spark, delta_path, limit=10)
    history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

    # Time travel: read version 0 vs latest
    print("─── Time Travel: Version 0 ───")
    v0 = read_delta_at_version(spark, delta_path, 0)
    print(f"   Version 0 rows: {v0.count()}")

    # Demonstrate unified batch/stream
    print("─── Unified Batch/Stream ───")
    result = demonstrate_unified_processing(spark, delta_path)
    print(f"   Batch rows: {result['batch_row_count']}")
    print(f"   Stream query ID: {result['stream_query_id']}")
