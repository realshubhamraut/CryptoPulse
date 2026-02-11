"""
CryptoPulse - Bronze Layer Trade Pipeline

Spark Structured Streaming job that ingests raw trade events
from Kafka and persists them to Delta Lake Bronze layer.
"""

from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_date,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    TimestampType,
)

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="bronze_trades")


# =============================================================================
# Schema Definitions
# =============================================================================

RAW_TRADE_SCHEMA = StructType([
    StructField("e", StringType(), True),      # Event type
    StructField("E", LongType(), True),        # Event time (ms)
    StructField("s", StringType(), True),      # Symbol
    StructField("t", LongType(), True),        # Trade ID
    StructField("p", StringType(), True),      # Price
    StructField("q", StringType(), True),      # Quantity
    StructField("b", LongType(), True),        # Buyer order ID
    StructField("a", LongType(), True),        # Seller order ID
    StructField("T", LongType(), True),        # Trade time (ms)
    StructField("m", BooleanType(), True),     # Is buyer maker
    StructField("M", BooleanType(), True),     # Ignore
])


class BronzeTradesPipeline:
    """
    Bronze layer pipeline for raw trade events.
    
    Consumes from Kafka, applies minimal transformation,
    and writes to Delta Lake with partitioning.
    """
    
    def __init__(
        self,
        spark: SparkSession | None = None,
        checkpoint_location: str | None = None,
        output_path: str | None = None,
    ):
        self.spark = spark or self._create_spark_session()
        self.checkpoint_location = (
            checkpoint_location or f"{settings.storage.delta_lake_path}/checkpoints/bronze_trades"
        )
        self.output_path = output_path or f"{settings.storage.delta_lake_path}/bronze/trades"
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake and Kafka support."""
        return (
            SparkSession.builder
            .appName("CryptoPulse-Bronze-Trades")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.adaptive.enabled", "true")
            # Kafka configs
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "io.delta:delta-spark_2.12:3.0.0")
            .getOrCreate()
        )
    
    def read_from_kafka(self) -> DataFrame:
        """Read streaming data from Kafka."""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
            .option("subscribe", settings.kafka.trades_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 100000)  # Rate limiting
            .load()
        )
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform raw Kafka messages to Bronze schema.
        
        Minimal transformation - preserves raw data fidelity.
        """
        return (
            df
            # Parse JSON value
            .select(
                col("key").cast("string").alias("kafka_key"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                from_json(col("value").cast("string"), RAW_TRADE_SCHEMA).alias("data")
            )
            # Flatten and add metadata
            .select(
                # Original trade data
                col("data.e").alias("event_type"),
                col("data.E").alias("event_time_ms"),
                col("data.s").alias("symbol"),
                col("data.t").alias("trade_id"),
                col("data.p").alias("price"),
                col("data.q").alias("quantity"),
                col("data.b").alias("buyer_order_id"),
                col("data.a").alias("seller_order_id"),
                col("data.T").alias("trade_time_ms"),
                col("data.m").alias("is_buyer_maker"),
                # Kafka metadata
                col("kafka_key"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("kafka_timestamp"),
                # Processing metadata
                current_timestamp().alias("ingested_at"),
            )
            # Add partition columns
            .withColumn(
                "trade_date",
                to_date((col("trade_time_ms") / 1000).cast(TimestampType()))
            )
        )
    
    def write_to_delta(self, df: DataFrame) -> Any:
        """Write streaming DataFrame to Delta Lake."""
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .partitionBy("trade_date", "symbol")
            .trigger(processingTime="10 seconds")
            .start(self.output_path)
        )
    
    def run(self) -> Any:
        """Run the streaming pipeline."""
        logger.info(
            "starting_bronze_trades_pipeline",
            output_path=self.output_path,
            checkpoint=self.checkpoint_location,
        )
        
        raw_df = self.read_from_kafka()
        transformed_df = self.transform(raw_df)
        query = self.write_to_delta(transformed_df)
        
        logger.info("bronze_trades_pipeline_started", query_id=str(query.id))
        
        return query
    
    def run_and_await(self) -> None:
        """Run pipeline and wait for termination."""
        query = self.run()
        query.awaitTermination()


# =============================================================================
# Batch Mode (for backfill/reprocessing)
# =============================================================================

def backfill_trades(
    spark: SparkSession,
    source_path: str,
    target_path: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """
    Batch backfill of trade data to Bronze layer.
    
    Args:
        spark: SparkSession
        source_path: Path to source JSON files
        target_path: Delta table path
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
    """
    logger.info(
        "starting_trades_backfill",
        source=source_path,
        target=target_path,
    )
    
    # Read batch data
    df = spark.read.json(source_path, schema=RAW_TRADE_SCHEMA)
    
    # Apply date filter if specified
    if start_date or end_date:
        df = df.withColumn(
            "trade_date",
            to_date((col("T") / 1000).cast(TimestampType()))
        )
        if start_date:
            df = df.filter(col("trade_date") >= start_date)
        if end_date:
            df = df.filter(col("trade_date") <= end_date)
    
    # Transform to Bronze schema
    transformed_df = (
        df.select(
            col("e").alias("event_type"),
            col("E").alias("event_time_ms"),
            col("s").alias("symbol"),
            col("t").alias("trade_id"),
            col("p").alias("price"),
            col("q").alias("quantity"),
            col("b").alias("buyer_order_id"),
            col("a").alias("seller_order_id"),
            col("T").alias("trade_time_ms"),
            col("m").alias("is_buyer_maker"),
            lit(None).alias("kafka_key"),
            lit(None).alias("kafka_topic"),
            lit(None).alias("kafka_partition"),
            lit(None).alias("kafka_offset"),
            lit(None).cast(TimestampType()).alias("kafka_timestamp"),
            current_timestamp().alias("ingested_at"),
        )
        .withColumn(
            "trade_date",
            to_date((col("trade_time_ms") / 1000).cast(TimestampType()))
        )
    )
    
    # Write to Delta
    (
        transformed_df.write
        .format("delta")
        .mode("append")
        .partitionBy("trade_date", "symbol")
        .save(target_path)
    )
    
    logger.info("trades_backfill_complete", records=transformed_df.count())


if __name__ == "__main__":
    pipeline = BronzeTradesPipeline()
    pipeline.run_and_await()
