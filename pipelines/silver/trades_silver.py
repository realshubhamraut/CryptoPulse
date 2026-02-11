"""
CryptoPulse - Silver Layer Trade Pipeline

Transforms Bronze trade data to Silver layer with:
- Data cleansing and validation
- Timestamp normalization (UTC)
- Deduplication by trade_id
- Schema enforcement
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_timestamp,
    current_timestamp,
    when,
    lit,
    row_number,
)
from pyspark.sql.types import DecimalType, TimestampType
from pyspark.sql.window import Window

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="silver_trades")


class SilverTradesPipeline:
    """
    Silver layer pipeline for cleansed trade data.
    
    Reads from Bronze Delta table, applies business logic,
    and writes to Silver Delta table.
    """
    
    def __init__(
        self,
        spark: SparkSession | None = None,
        checkpoint_location: str | None = None,
        bronze_path: str | None = None,
        output_path: str | None = None,
    ):
        self.spark = spark or self._create_spark_session()
        self.checkpoint_location = (
            checkpoint_location or f"{settings.storage.delta_lake_path}/checkpoints/silver_trades"
        )
        self.bronze_path = bronze_path or f"{settings.storage.delta_lake_path}/bronze/trades"
        self.output_path = output_path or f"{settings.storage.delta_lake_path}/silver/trades"
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake support."""
        return (
            SparkSession.builder
            .appName("CryptoPulse-Silver-Trades")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .getOrCreate()
        )
    
    def read_from_bronze(self) -> DataFrame:
        """Read streaming data from Bronze Delta table."""
        return (
            self.spark.readStream
            .format("delta")
            .option("maxFilesPerTrigger", 100)
            .load(self.bronze_path)
        )
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply Silver layer transformations.
        
        - Convert timestamps to proper datetime
        - Cast numeric columns to appropriate types
        - Add derived columns
        - Handle data quality issues
        """
        # Window for deduplication
        dedup_window = Window.partitionBy("symbol", "trade_id").orderBy(col("ingested_at").desc())
        
        return (
            df
            # Data quality: filter out null trade_ids
            .filter(col("trade_id").isNotNull())
            .filter(col("symbol").isNotNull())
            .filter(col("price").isNotNull())
            .filter(col("quantity").isNotNull())
            
            # Convert timestamps
            .withColumn(
                "trade_timestamp",
                to_timestamp(col("trade_time_ms") / 1000)
            )
            .withColumn(
                "event_timestamp",
                to_timestamp(col("event_time_ms") / 1000)
            )
            
            # Cast numeric columns
            .withColumn("price", col("price").cast(DecimalType(38, 8)))
            .withColumn("quantity", col("quantity").cast(DecimalType(38, 8)))
            
            # Calculate quote quantity (price * quantity)
            .withColumn("quote_quantity", col("price") * col("quantity"))
            
            # Determine trade side
            # is_buyer_maker = True means buyer's order was on the book,
            # so the trade was initiated by a seller (SELL)
            .withColumn(
                "side",
                when(col("is_buyer_maker"), lit("SELL")).otherwise(lit("BUY"))
            )
            
            # Deduplication: keep latest version of each trade
            .withColumn("row_num", row_number().over(dedup_window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            
            # Add processing metadata
            .withColumn("processed_at", current_timestamp())
            
            # Select final schema
            .select(
                col("trade_id"),
                col("symbol"),
                col("price"),
                col("quantity"),
                col("quote_quantity"),
                col("side"),
                col("trade_timestamp"),
                col("event_timestamp"),
                col("buyer_order_id"),
                col("seller_order_id"),
                col("trade_date"),
                col("ingested_at"),
                col("processed_at"),
            )
        )
    
    def write_to_delta(self, df: DataFrame):
        """Write streaming DataFrame to Silver Delta table."""
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .option("mergeSchema", "true")
            .partitionBy("trade_date", "symbol")
            .trigger(processingTime="15 seconds")
            .start(self.output_path)
        )
    
    def run(self):
        """Run the streaming pipeline."""
        logger.info(
            "starting_silver_trades_pipeline",
            bronze_path=self.bronze_path,
            output_path=self.output_path,
        )
        
        bronze_df = self.read_from_bronze()
        silver_df = self.transform(bronze_df)
        query = self.write_to_delta(silver_df)
        
        logger.info("silver_trades_pipeline_started", query_id=str(query.id))
        
        return query
    
    def run_and_await(self) -> None:
        """Run pipeline and wait for termination."""
        query = self.run()
        query.awaitTermination()


if __name__ == "__main__":
    pipeline = SilverTradesPipeline()
    pipeline.run_and_await()
