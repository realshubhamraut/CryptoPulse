"""
CryptoPulse - Pipeline Utilities

Common utilities for Spark streaming pipelines.
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
)
from pyspark.sql.types import DecimalType

from cryptopulse.config import settings


def create_spark_session(
    app_name: str = "CryptoPulse",
    enable_delta: bool = True,
    enable_kafka: bool = True,
) -> SparkSession:
    """
    Create a configured Spark session.
    
    Args:
        app_name: Spark application name
        enable_delta: Enable Delta Lake support
        enable_kafka: Enable Kafka support
        
    Returns:
        Configured SparkSession
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
    )
    
    packages = []
    
    if enable_delta:
        builder = builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        packages.append("io.delta:delta-spark_2.12:3.0.0")
    
    if enable_kafka:
        packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    
    # MinIO / S3 configuration for local development
    if settings.is_local:
        builder = builder.config(
            "spark.hadoop.fs.s3a.endpoint", f"http://{settings.storage.minio_endpoint}"
        ).config(
            "spark.hadoop.fs.s3a.access.key", settings.storage.minio_access_key
        ).config(
            "spark.hadoop.fs.s3a.secret.key", settings.storage.minio_secret_key
        ).config(
            "spark.hadoop.fs.s3a.path.style.access", "true"
        ).config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
    
    return builder.getOrCreate()


def compute_ohlc(
    df: DataFrame,
    timestamp_col: str = "trade_time_ms",
    price_col: str = "price",
    quantity_col: str = "quantity",
    window_duration: str = "1 minute",
    watermark_duration: str = "10 seconds",
) -> DataFrame:
    """
    Compute OHLC candlestick aggregations.
    
    Args:
        df: Input DataFrame with trade data
        timestamp_col: Column containing trade timestamp (ms)
        price_col: Column containing price
        quantity_col: Column containing quantity
        window_duration: Tumbling window duration
        watermark_duration: Watermark for late data handling
        
    Returns:
        DataFrame with OHLC aggregations
    """
    from pyspark.sql.functions import to_timestamp
    
    return (
        df
        .withColumn(
            "trade_timestamp",
            to_timestamp(col(timestamp_col) / 1000)
        )
        .withColumn("price_decimal", col(price_col).cast(DecimalType(38, 8)))
        .withColumn("quantity_decimal", col(quantity_col).cast(DecimalType(38, 8)))
        .withColumn(
            "quote_quantity",
            col("price_decimal") * col("quantity_decimal")
        )
        .withWatermark("trade_timestamp", watermark_duration)
        .groupBy(
            col("symbol"),
            window(col("trade_timestamp"), window_duration).alias("window")
        )
        .agg(
            first("price_decimal").alias("open"),
            spark_max("price_decimal").alias("high"),
            spark_min("price_decimal").alias("low"),
            last("price_decimal").alias("close"),
            spark_sum("quantity_decimal").alias("volume"),
            spark_sum("quote_quantity").alias("quote_volume"),
            count("*").alias("trade_count"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("quote_volume"),
            col("trade_count"),
        )
    )


def compute_vwap(
    df: DataFrame,
    timestamp_col: str = "trade_time_ms",
    price_col: str = "price",
    quantity_col: str = "quantity",
    window_duration: str = "5 minutes",
    watermark_duration: str = "30 seconds",
) -> DataFrame:
    """
    Compute Volume Weighted Average Price.
    
    VWAP = sum(price * quantity) / sum(quantity)
    
    Args:
        df: Input DataFrame with trade data
        timestamp_col: Column containing trade timestamp (ms)
        price_col: Column containing price
        quantity_col: Column containing quantity
        window_duration: Tumbling window duration
        watermark_duration: Watermark for late data handling
        
    Returns:
        DataFrame with VWAP calculations
    """
    from pyspark.sql.functions import to_timestamp
    
    return (
        df
        .withColumn(
            "trade_timestamp",
            to_timestamp(col(timestamp_col) / 1000)
        )
        .withColumn("price_decimal", col(price_col).cast(DecimalType(38, 8)))
        .withColumn("quantity_decimal", col(quantity_col).cast(DecimalType(38, 8)))
        .withColumn(
            "price_volume",
            col("price_decimal") * col("quantity_decimal")
        )
        .withWatermark("trade_timestamp", watermark_duration)
        .groupBy(
            col("symbol"),
            window(col("trade_timestamp"), window_duration).alias("window")
        )
        .agg(
            spark_sum("price_volume").alias("total_price_volume"),
            spark_sum("quantity_decimal").alias("total_volume"),
        )
        .withColumn(
            "vwap",
            when(col("total_volume") > 0,
                 col("total_price_volume") / col("total_volume"))
            .otherwise(None)
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("vwap"),
            col("total_volume").alias("volume"),
        )
    )


def compute_volatility(
    df: DataFrame,
    timestamp_col: str = "trade_time_ms",
    price_col: str = "price",
    window_duration: str = "5 minutes",
    watermark_duration: str = "30 seconds",
) -> DataFrame:
    """
    Compute rolling volatility (standard deviation of returns).
    
    Args:
        df: Input DataFrame with trade data
        timestamp_col: Column containing trade timestamp (ms)
        price_col: Column containing price
        window_duration: Tumbling window duration
        watermark_duration: Watermark for late data handling
        
    Returns:
        DataFrame with volatility calculations
    """
    from pyspark.sql.functions import to_timestamp, log
    
    return (
        df
        .withColumn(
            "trade_timestamp",
            to_timestamp(col(timestamp_col) / 1000)
        )
        .withColumn("price_decimal", col(price_col).cast(DecimalType(38, 8)))
        .withColumn("log_price", log(col("price_decimal")))
        .withWatermark("trade_timestamp", watermark_duration)
        .groupBy(
            col("symbol"),
            window(col("trade_timestamp"), window_duration).alias("window")
        )
        .agg(
            avg("price_decimal").alias("avg_price"),
            stddev("price_decimal").alias("price_stddev"),
            stddev("log_price").alias("log_return_stddev"),
            count("*").alias("sample_count"),
        )
        .withColumn(
            "volatility",
            when(col("avg_price") > 0,
                 col("price_stddev") / col("avg_price") * 100)  # as percentage
            .otherwise(None)
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("volatility"),
            col("log_return_stddev"),
            col("sample_count"),
        )
    )


def compute_buy_sell_ratio(
    df: DataFrame,
    timestamp_col: str = "trade_time_ms",
    quantity_col: str = "quantity",
    is_buyer_maker_col: str = "is_buyer_maker",
    window_duration: str = "1 minute",
    watermark_duration: str = "10 seconds",
) -> DataFrame:
    """
    Compute buy/sell volume ratio.
    
    When is_buyer_maker is True, it means the buyer placed the order first,
    so the trade was initiated by a seller (market sell).
    When is_buyer_maker is False, the trade was initiated by a buyer (market buy).
    
    Args:
        df: Input DataFrame with trade data
        timestamp_col: Column containing trade timestamp (ms)
        quantity_col: Column containing quantity
        is_buyer_maker_col: Column indicating if buyer is maker
        window_duration: Tumbling window duration
        watermark_duration: Watermark for late data handling
        
    Returns:
        DataFrame with buy/sell ratio
    """
    from pyspark.sql.functions import to_timestamp
    
    return (
        df
        .withColumn(
            "trade_timestamp",
            to_timestamp(col(timestamp_col) / 1000)
        )
        .withColumn("quantity_decimal", col(quantity_col).cast(DecimalType(38, 8)))
        .withColumn(
            "buy_volume",
            when(~col(is_buyer_maker_col), col("quantity_decimal")).otherwise(0)
        )
        .withColumn(
            "sell_volume",
            when(col(is_buyer_maker_col), col("quantity_decimal")).otherwise(0)
        )
        .withWatermark("trade_timestamp", watermark_duration)
        .groupBy(
            col("symbol"),
            window(col("trade_timestamp"), window_duration).alias("window")
        )
        .agg(
            spark_sum("buy_volume").alias("total_buy_volume"),
            spark_sum("sell_volume").alias("total_sell_volume"),
            spark_sum("quantity_decimal").alias("total_volume"),
        )
        .withColumn(
            "buy_sell_ratio",
            when(col("total_volume") > 0,
                 col("total_buy_volume") / col("total_volume"))
            .otherwise(0.5)
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("buy_sell_ratio"),
            col("total_buy_volume"),
            col("total_sell_volume"),
            col("total_volume"),
        )
    )
