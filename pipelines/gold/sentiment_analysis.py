"""
CryptoPulse - Gold Layer Sentiment Analysis Pipeline

Processes Silver news data through NLP sentiment analysis:
- Financial sentiment classification (positive/negative/neutral)
- Confidence scoring
- Aggregation by trading pair and time window
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    window,
    avg,
    sum as spark_sum,
    count,
    when,
    lit,
    explode,
    current_timestamp,
    udf,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
)

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="gold_sentiment")


# =============================================================================
# Sentiment Analysis UDF
# =============================================================================

# Sentiment output schema
SENTIMENT_SCHEMA = StructType([
    StructField("label", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("confidence", FloatType(), True),
])


def create_sentiment_analyzer():
    """
    Create a sentiment analysis function using FinBERT or a simpler model.
    
    Note: In production, this would use a distributed model serving solution
    or pre-computed embeddings. For streaming, we use a lightweight approach.
    """
    try:
        from transformers import pipeline
        
        # Use FinBERT for financial sentiment
        classifier = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert",
            device=-1,  # CPU
            truncation=True,
            max_length=512,
        )
        
        def analyze(text: str) -> dict:
            if not text or len(text.strip()) < 10:
                return {"label": "neutral", "score": 0.0, "confidence": 0.5}
            
            try:
                result = classifier(text[:512])[0]
                label = result["label"].lower()
                score_map = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}
                
                return {
                    "label": label,
                    "score": score_map.get(label, 0.0) * result["score"],
                    "confidence": result["score"],
                }
            except Exception:
                return {"label": "neutral", "score": 0.0, "confidence": 0.5}
        
        return analyze
        
    except Exception as e:
        logger.warning("finbert_not_available", error=str(e), fallback="rule_based")
        
        # Fallback to simple rule-based sentiment
        positive_words = {"bullish", "surge", "rally", "gain", "rise", "pump", "moon", "break", "ath"}
        negative_words = {"bearish", "crash", "dump", "fall", "drop", "plunge", "sell", "fear", "hack"}
        
        def analyze(text: str) -> dict:
            if not text:
                return {"label": "neutral", "score": 0.0, "confidence": 0.5}
            
            text_lower = text.lower()
            pos_count = sum(1 for w in positive_words if w in text_lower)
            neg_count = sum(1 for w in negative_words if w in text_lower)
            
            total = pos_count + neg_count
            if total == 0:
                return {"label": "neutral", "score": 0.0, "confidence": 0.5}
            
            score = (pos_count - neg_count) / total
            
            if score > 0.2:
                label = "positive"
            elif score < -0.2:
                label = "negative"
            else:
                label = "neutral"
            
            return {
                "label": label,
                "score": score,
                "confidence": min(0.5 + total * 0.1, 0.9),
            }
        
        return analyze


class SentimentAnalysisPipeline:
    """
    Gold layer pipeline for news sentiment analysis.
    
    Processes Silver news articles and produces:
    - Per-article sentiment scores
    - Per-symbol aggregated sentiment windows
    """
    
    def __init__(
        self,
        spark: SparkSession | None = None,
        checkpoint_location: str | None = None,
        silver_path: str | None = None,
        output_path: str | None = None,
    ):
        self.spark = spark or self._create_spark_session()
        self.checkpoint_location = (
            checkpoint_location or f"{settings.storage.adls_delta_path}/checkpoints/gold_sentiment"
        )
        self.silver_path = silver_path or f"{settings.storage.adls_delta_path}/silver/news"
        self.output_path = output_path or f"{settings.storage.adls_delta_path}/gold/sentiment"
        
        # Register sentiment UDF
        self._register_sentiment_udf()
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session."""
        return (
            SparkSession.builder
            .appName("CryptoPulse-Gold-Sentiment")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .getOrCreate()
        )
    
    def _register_sentiment_udf(self) -> None:
        """Register the sentiment analysis UDF."""
        analyzer = create_sentiment_analyzer()
        
        @udf(returnType=SENTIMENT_SCHEMA)
        def analyze_sentiment(text: str):
            return analyzer(text)
        
        self.sentiment_udf = analyze_sentiment
    
    def read_from_silver(self) -> DataFrame:
        """Read streaming data from Silver Delta table."""
        return (
            self.spark.readStream
            .format("delta")
            .option("maxFilesPerTrigger", 20)
            .load(self.silver_path)
        )
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply sentiment analysis to news articles.
        
        Produces per-article sentiment scores and explodes by mentioned symbols.
        """
        return (
            df
            # Analyze sentiment on title + content excerpt
            .withColumn(
                "sentiment_result",
                self.sentiment_udf(col("title") + ". " + col("content"))
            )
            # Extract sentiment fields
            .withColumn("sentiment_label", col("sentiment_result.label"))
            .withColumn("sentiment_score", col("sentiment_result.score"))
            .withColumn("sentiment_confidence", col("sentiment_result.confidence"))
            
            # Explode mentioned symbols to create per-symbol rows
            .withColumn("mentioned_symbol", explode(col("mentioned_symbols")))
            
            # Add processing timestamp
            .withColumn("analyzed_at", current_timestamp())
            
            # Select output columns
            .select(
                col("article_id"),
                col("source"),
                col("title"),
                col("mentioned_symbol").alias("symbol"),
                col("sentiment_label"),
                col("sentiment_score"),
                col("sentiment_confidence"),
                col("published_timestamp"),
                col("analyzed_at"),
                col("word_count"),
                col("content_hash"),
            )
        )
    
    def write_to_delta(self, df: DataFrame):
        """Write streaming DataFrame to Gold Delta table."""
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .option("mergeSchema", "true")
            .partitionBy("symbol")
            .trigger(processingTime="60 seconds")
            .start(self.output_path)
        )
    
    def run(self):
        """Run the streaming pipeline."""
        logger.info(
            "starting_sentiment_analysis_pipeline",
            silver_path=self.silver_path,
            output_path=self.output_path,
        )
        
        silver_df = self.read_from_silver()
        sentiment_df = self.transform(silver_df)
        query = self.write_to_delta(sentiment_df)
        
        logger.info("sentiment_pipeline_started", query_id=str(query.id))
        
        return query
    
    def run_and_await(self) -> None:
        """Run pipeline and wait for termination."""
        query = self.run()
        query.awaitTermination()


# =============================================================================
# Aggregated Sentiment View
# =============================================================================

def compute_aggregated_sentiment(
    spark: SparkSession,
    sentiment_path: str,
    output_path: str,
    window_duration: str = "15 minutes",
) -> None:
    """
    Compute aggregated sentiment per symbol per time window.
    
    This runs as a batch job to create summary views.
    """
    logger.info("computing_aggregated_sentiment", window=window_duration)
    
    sentiment_df = spark.read.format("delta").load(sentiment_path)
    
    agg_df = (
        sentiment_df
        .groupBy(
            col("symbol"),
            window(col("published_timestamp"), window_duration).alias("time_window")
        )
        .agg(
            avg("sentiment_score").alias("mean_score"),
            # Confidence-weighted average
            (spark_sum(col("sentiment_score") * col("sentiment_confidence")) / 
             spark_sum("sentiment_confidence")).alias("weighted_score"),
            count("*").alias("article_count"),
            spark_sum(when(col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_count"),
            spark_sum(when(col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count"),
            spark_sum(when(col("sentiment_label") == "neutral", 1).otherwise(0)).alias("neutral_count"),
        )
        .withColumn(
            "dominant_label",
            when(col("positive_count") >= col("negative_count") & 
                 col("positive_count") >= col("neutral_count"), "positive")
            .when(col("negative_count") > col("positive_count") & 
                  col("negative_count") > col("neutral_count"), "negative")
            .otherwise("neutral")
        )
        .select(
            col("symbol"),
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("mean_score"),
            col("weighted_score"),
            col("article_count"),
            col("positive_count"),
            col("negative_count"),
            col("neutral_count"),
            col("dominant_label"),
        )
    )
    
    agg_df.write.format("delta").mode("overwrite").save(output_path)
    
    logger.info("aggregated_sentiment_complete", records=agg_df.count())


if __name__ == "__main__":
    pipeline = SentimentAnalysisPipeline()
    pipeline.run_and_await()
