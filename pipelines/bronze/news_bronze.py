"""
CryptoPulse - Bronze Layer News Pipeline

Spark Structured Streaming job that ingests raw news articles
from Kafka and persists them to Delta Lake Bronze layer.
"""

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
    ArrayType,
    TimestampType,
)

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="bronze_news")


# =============================================================================
# Schema Definitions
# =============================================================================

RAW_NEWS_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("article_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("content", StringType(), True),
    StructField("url", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("crawled_at", StringType(), True),
    StructField("author", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("content_hash", StringType(), True),
])


class BronzeNewsPipeline:
    """
    Bronze layer pipeline for raw news articles.
    
    Consumes from Kafka, preserves raw data,
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
            checkpoint_location or f"{settings.storage.delta_lake_path}/checkpoints/bronze_news"
        )
        self.output_path = output_path or f"{settings.storage.delta_lake_path}/bronze/news"
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake and Kafka support."""
        return (
            SparkSession.builder
            .appName("CryptoPulse-Bronze-News")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.streaming.schemaInference", "true")
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
            .option("subscribe", settings.kafka.news_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 10000)
            .load()
        )
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform raw Kafka messages to Bronze schema.
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
                from_json(col("value").cast("string"), RAW_NEWS_SCHEMA).alias("data")
            )
            # Flatten and add metadata
            .select(
                # Original article data
                col("data.source").alias("source"),
                col("data.article_id").alias("article_id"),
                col("data.title").alias("title"),
                col("data.content").alias("content"),
                col("data.url").alias("url"),
                col("data.published_at").alias("published_at"),
                col("data.crawled_at").alias("crawled_at"),
                col("data.author").alias("author"),
                col("data.tags").alias("tags"),
                col("data.content_hash").alias("content_hash"),
                # Kafka metadata
                col("kafka_key"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("kafka_timestamp"),
                # Processing metadata
                current_timestamp().alias("ingested_at"),
            )
            # Add partition columns (date and source)
            .withColumn(
                "publish_date",
                to_date(col("published_at").cast(TimestampType()))
            )
        )
    
    def write_to_delta(self, df: DataFrame):
        """Write streaming DataFrame to Delta Lake."""
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .partitionBy("publish_date", "source")
            .trigger(processingTime="30 seconds")  # News is less frequent
            .start(self.output_path)
        )
    
    def run(self):
        """Run the streaming pipeline."""
        logger.info(
            "starting_bronze_news_pipeline",
            output_path=self.output_path,
            checkpoint=self.checkpoint_location,
        )
        
        raw_df = self.read_from_kafka()
        transformed_df = self.transform(raw_df)
        query = self.write_to_delta(transformed_df)
        
        logger.info("bronze_news_pipeline_started", query_id=str(query.id))
        
        return query
    
    def run_and_await(self) -> None:
        """Run pipeline and wait for termination."""
        query = self.run()
        query.awaitTermination()


if __name__ == "__main__":
    pipeline = BronzeNewsPipeline()
    pipeline.run_and_await()
