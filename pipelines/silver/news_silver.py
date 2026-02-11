"""
CryptoPulse - Silver Layer News Pipeline

Transforms Bronze news data to Silver layer with:
- Text cleaning and normalization
- Entity extraction for cryptocurrency mentions
- Word count and content quality metrics
- Deduplication by content hash
"""

import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    current_timestamp,
    lower,
    regexp_replace,
    trim,
    length,
    split,
    size,
    array_distinct,
    udf,
    row_number,
)
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="silver_news")


# =============================================================================
# UDFs for Entity Extraction
# =============================================================================

# Common cryptocurrency symbols to extract from text
CRYPTO_SYMBOLS = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "AVAX", "DOT", "MATIC",
    "LINK", "UNI", "ATOM", "LTC", "ETC", "XLM", "NEAR", "APT", "FIL", "ARB",
    "OP", "INJ", "SUI", "SEI", "TIA", "PEPE", "SHIB", "BONK",
    "BITCOIN", "ETHEREUM", "BINANCE", "SOLANA", "RIPPLE", "CARDANO", "DOGECOIN",
}

SYMBOL_PATTERN = re.compile(r'\b(' + '|'.join(CRYPTO_SYMBOLS) + r')\b', re.IGNORECASE)


def extract_crypto_mentions(text: str) -> list[str]:
    """Extract cryptocurrency symbol mentions from text."""
    if not text:
        return []
    
    matches = SYMBOL_PATTERN.findall(text)
    # Normalize to uppercase symbols
    normalized = set()
    for match in matches:
        upper = match.upper()
        # Map full names to symbols
        name_map = {
            "BITCOIN": "BTC", "ETHEREUM": "ETH", "BINANCE": "BNB",
            "SOLANA": "SOL", "RIPPLE": "XRP", "CARDANO": "ADA", "DOGECOIN": "DOGE"
        }
        normalized.add(name_map.get(upper, upper))
    
    return sorted(list(normalized))


# Register UDF
extract_crypto_udf = udf(extract_crypto_mentions, ArrayType(StringType()))


class SilverNewsPipeline:
    """
    Silver layer pipeline for cleansed news articles.
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
            checkpoint_location or f"{settings.storage.delta_lake_path}/checkpoints/silver_news"
        )
        self.bronze_path = bronze_path or f"{settings.storage.delta_lake_path}/bronze/news"
        self.output_path = output_path or f"{settings.storage.delta_lake_path}/silver/news"
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session."""
        return (
            SparkSession.builder
            .appName("CryptoPulse-Silver-News")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .getOrCreate()
        )
    
    def read_from_bronze(self) -> DataFrame:
        """Read streaming data from Bronze Delta table."""
        return (
            self.spark.readStream
            .format("delta")
            .option("maxFilesPerTrigger", 50)
            .load(self.bronze_path)
        )
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply Silver layer transformations."""
        # Deduplication window
        dedup_window = Window.partitionBy("content_hash").orderBy(col("ingested_at").desc())
        
        return (
            df
            # Data quality filters
            .filter(col("article_id").isNotNull())
            .filter(col("title").isNotNull())
            .filter(length(col("title")) > 10)
            .filter(col("content").isNotNull())
            .filter(length(col("content")) > 50)
            
            # Clean text
            .withColumn(
                "title_clean",
                trim(regexp_replace(col("title"), r'\s+', ' '))
            )
            .withColumn(
                "content_clean",
                trim(regexp_replace(col("content"), r'\s+', ' '))
            )
            # Remove HTML tags
            .withColumn(
                "content_clean",
                regexp_replace(col("content_clean"), r'<[^>]+>', '')
            )
            
            # Parse timestamps
            .withColumn(
                "published_timestamp",
                to_timestamp(col("published_at"))
            )
            .withColumn(
                "crawled_timestamp",
                to_timestamp(col("crawled_at"))
            )
            
            # Calculate word count
            .withColumn(
                "word_count",
                size(split(col("content_clean"), r'\s+'))
            )
            
            # Extract cryptocurrency mentions
            .withColumn(
                "mentioned_symbols",
                array_distinct(
                    extract_crypto_udf(
                        col("title_clean") + " " + col("content_clean")
                    )
                )
            )
            
            # Deduplication
            .withColumn("row_num", row_number().over(dedup_window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            
            # Add processing metadata
            .withColumn("processed_at", current_timestamp())
            
            # Select final schema
            .select(
                col("article_id"),
                col("source"),
                col("title_clean").alias("title"),
                col("content_clean").alias("content"),
                col("url"),
                col("author"),
                col("tags"),
                col("published_timestamp"),
                col("crawled_timestamp"),
                col("mentioned_symbols"),
                col("word_count"),
                col("content_hash"),
                col("publish_date"),
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
            .partitionBy("publish_date", "source")
            .trigger(processingTime="60 seconds")
            .start(self.output_path)
        )
    
    def run(self):
        """Run the streaming pipeline."""
        logger.info(
            "starting_silver_news_pipeline",
            bronze_path=self.bronze_path,
            output_path=self.output_path,
        )
        
        bronze_df = self.read_from_bronze()
        silver_df = self.transform(bronze_df)
        query = self.write_to_delta(silver_df)
        
        logger.info("silver_news_pipeline_started", query_id=str(query.id))
        
        return query
    
    def run_and_await(self) -> None:
        """Run pipeline and wait for termination."""
        query = self.run()
        query.awaitTermination()


if __name__ == "__main__":
    pipeline = SilverNewsPipeline()
    pipeline.run_and_await()
