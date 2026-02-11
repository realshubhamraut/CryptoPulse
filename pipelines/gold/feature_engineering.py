"""
CryptoPulse - Gold Layer Feature Engineering Pipeline

Computes market microstructure features from Silver trade data:
- OHLC candlesticks (1min, 5min, 15min)
- VWAP (Volume Weighted Average Price)
- Rolling volatility
- Trade intensity metrics
- Buy/sell ratio
- Order flow features
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    window,
    first,
    last,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    count,
    avg,
    stddev,
    when,
    lit,
    struct,
    explode,
    array,
    current_timestamp,
)
from pyspark.sql.types import DecimalType

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="gold_features")


class FeatureEngineeringPipeline:
    """
    Gold layer pipeline for market microstructure features.
    
    Reads from Silver trade data and computes:
    - Multi-interval OHLC (1m, 5m, 15m)
    - VWAP per interval
    - Rolling volatility
    - Trade intensity
    - Buy/sell pressure ratio
    """
    
    INTERVALS = ["1 minute", "5 minutes", "15 minutes"]
    
    def __init__(
        self,
        spark: SparkSession | None = None,
        checkpoint_location: str | None = None,
        silver_path: str | None = None,
        output_path: str | None = None,
    ):
        self.spark = spark or self._create_spark_session()
        self.checkpoint_location = (
            checkpoint_location or f"{settings.storage.delta_lake_path}/checkpoints/gold_features"
        )
        self.silver_path = silver_path or f"{settings.storage.delta_lake_path}/silver/trades"
        self.output_path = output_path or f"{settings.storage.delta_lake_path}/gold/features"
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session."""
        return (
            SparkSession.builder
            .appName("CryptoPulse-Gold-Features")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .getOrCreate()
        )
    
    def read_from_silver(self) -> DataFrame:
        """Read streaming data from Silver Delta table."""
        return (
            self.spark.readStream
            .format("delta")
            .option("maxFilesPerTrigger", 200)
            .load(self.silver_path)
        )
    
    def compute_features_for_interval(
        self,
        df: DataFrame,
        interval: str,
        watermark: str = "30 seconds",
    ) -> DataFrame:
        """
        Compute all features for a specific time interval.
        
        Args:
            df: Input DataFrame with trade data
            interval: Window interval (e.g., "1 minute")
            watermark: Watermark duration for late data
            
        Returns:
            DataFrame with computed features
        """
        interval_short = interval.replace(" minute", "m").replace("s", "")
        
        return (
            df
            .withWatermark("trade_timestamp", watermark)
            .withColumn(
                "buy_quantity",
                when(col("side") == "BUY", col("quantity")).otherwise(lit(0))
            )
            .withColumn(
                "sell_quantity",
                when(col("side") == "SELL", col("quantity")).otherwise(lit(0))
            )
            .groupBy(
                col("symbol"),
                window(col("trade_timestamp"), interval).alias("time_window")
            )
            .agg(
                # OHLC
                first("price").alias("open"),
                spark_max("price").alias("high"),
                spark_min("price").alias("low"),
                last("price").alias("close"),
                
                # Volume
                spark_sum("quantity").alias("volume"),
                spark_sum("quote_quantity").alias("quote_volume"),
                spark_sum("buy_quantity").alias("buy_volume"),
                spark_sum("sell_quantity").alias("sell_volume"),
                
                # Trade count
                count("*").alias("trade_count"),
                
                # VWAP components
                spark_sum(col("price") * col("quantity")).alias("price_volume_sum"),
                
                # Volatility components
                avg("price").alias("avg_price"),
                stddev("price").alias("price_stddev"),
            )
            # Compute derived features
            .withColumn(
                "vwap",
                when(col("volume") > 0,
                     col("price_volume_sum") / col("volume"))
                .otherwise(col("close"))
            )
            .withColumn(
                "volatility",
                when(col("avg_price") > 0,
                     col("price_stddev") / col("avg_price") * 100)
                .otherwise(lit(0.0))
            )
            .withColumn(
                "buy_sell_ratio",
                when(col("volume") > 0,
                     col("buy_volume") / col("volume"))
                .otherwise(lit(0.5))
            )
            .withColumn(
                "price_change",
                col("close") - col("open")
            )
            .withColumn(
                "price_change_pct",
                when(col("open") > 0,
                     (col("close") - col("open")) / col("open") * 100)
                .otherwise(lit(0.0))
            )
            .withColumn(
                "high_low_range",
                when(col("low") > 0,
                     (col("high") - col("low")) / col("low") * 100)
                .otherwise(lit(0.0))
            )
            # Calculate trade intensity (trades per second)
            .withColumn(
                "trade_intensity",
                col("trade_count") / lit(self._interval_to_seconds(interval))
            )
            # Add interval identifier
            .withColumn("interval", lit(interval_short))
            # Select final columns
            .select(
                col("symbol"),
                col("interval"),
                col("time_window.start").alias("window_start"),
                col("time_window.end").alias("window_end"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("volume"),
                col("quote_volume"),
                col("buy_volume"),
                col("sell_volume"),
                col("trade_count"),
                col("vwap"),
                col("volatility"),
                col("buy_sell_ratio"),
                col("price_change"),
                col("price_change_pct"),
                col("high_low_range"),
                col("trade_intensity"),
            )
        )
    
    def _interval_to_seconds(self, interval: str) -> int:
        """Convert interval string to seconds."""
        if "minute" in interval:
            return int(interval.split()[0]) * 60
        elif "hour" in interval:
            return int(interval.split()[0]) * 3600
        return 60
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Compute features for all intervals and union them.
        
        Note: In streaming mode, we need to handle each interval separately
        and union the results.
        """
        # For streaming, we'll compute the primary 1-minute interval
        # Other intervals can be derived downstream or computed in batch
        return self.compute_features_for_interval(df, "1 minute", "10 seconds")
    
    def write_to_delta(self, df: DataFrame):
        """Write streaming DataFrame to Gold Delta table."""
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .option("mergeSchema", "true")
            .partitionBy("symbol")
            .trigger(processingTime="10 seconds")
            .start(self.output_path)
        )
    
    def run(self):
        """Run the streaming pipeline."""
        logger.info(
            "starting_feature_engineering_pipeline",
            silver_path=self.silver_path,
            output_path=self.output_path,
        )
        
        silver_df = self.read_from_silver()
        features_df = self.transform(silver_df)
        query = self.write_to_delta(features_df)
        
        logger.info("feature_pipeline_started", query_id=str(query.id))
        
        return query
    
    def run_and_await(self) -> None:
        """Run pipeline and wait for termination."""
        query = self.run()
        query.awaitTermination()


# =============================================================================
# Batch Aggregation for Longer Intervals
# =============================================================================

def aggregate_to_higher_interval(
    spark: SparkSession,
    source_interval: str,
    target_interval: str,
    source_path: str,
    target_path: str,
) -> None:
    """
    Aggregate features from lower to higher time interval.
    
    For example: 1-minute features -> 5-minute features
    This runs as a batch job (hourly/daily).
    """
    logger.info(
        "aggregating_intervals",
        source=source_interval,
        target=target_interval,
    )
    
    source_df = spark.read.format("delta").load(source_path)
    
    # Filter to source interval
    source_df = source_df.filter(col("interval") == source_interval)
    
    target_short = target_interval.replace(" minute", "m").replace("s", "")
    
    # Aggregate to target interval
    result_df = (
        source_df
        .groupBy(
            col("symbol"),
            window(col("window_start"), target_interval).alias("time_window")
        )
        .agg(
            first("open").alias("open"),
            spark_max("high").alias("high"),
            spark_min("low").alias("low"),
            last("close").alias("close"),
            spark_sum("volume").alias("volume"),
            spark_sum("quote_volume").alias("quote_volume"),
            spark_sum("buy_volume").alias("buy_volume"),
            spark_sum("sell_volume").alias("sell_volume"),
            spark_sum("trade_count").alias("trade_count"),
            # Weighted VWAP
            (spark_sum(col("vwap") * col("volume")) / spark_sum("volume")).alias("vwap"),
            avg("volatility").alias("volatility"),
            (spark_sum("buy_volume") / spark_sum("volume")).alias("buy_sell_ratio"),
        )
        .withColumn("interval", lit(target_short))
        .withColumn(
            "price_change_pct",
            when(col("open") > 0, (col("close") - col("open")) / col("open") * 100)
            .otherwise(0)
        )
        .select(
            col("symbol"),
            col("interval"),
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("quote_volume"),
            col("buy_volume"),
            col("sell_volume"),
            col("trade_count"),
            col("vwap"),
            col("volatility"),
            col("buy_sell_ratio"),
            col("price_change_pct"),
        )
    )
    
    # Append to target
    result_df.write.format("delta").mode("append").save(target_path)
    
    logger.info("interval_aggregation_complete", records=result_df.count())


if __name__ == "__main__":
    pipeline = FeatureEngineeringPipeline()
    pipeline.run_and_await()
