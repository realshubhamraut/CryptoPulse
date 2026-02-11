# Databricks notebook source
# MAGIC %md
# MAGIC # 🧠 05 — Sentiment Analysis
# MAGIC
# MAGIC **CryptoPulse** | NLP-powered cryptocurrency news sentiment pipeline
# MAGIC
# MAGIC Applies FinBERT sentiment analysis to Silver news articles and produces
# MAGIC aggregated sentiment scores per symbol in Gold Delta tables.
# MAGIC
# MAGIC | Component | Technology |
# MAGIC |-----------|-----------|
# MAGIC | Model | FinBERT (ProsusAI/finbert) |
# MAGIC | Framework | HuggingFace Transformers (Spark UDF) |
# MAGIC | Aggregation | Per-symbol weighted sentiment |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")

ENV = dbutils.widgets.get("environment")
STORAGE = f"stcryptopulse{ENV}"
SILVER_NEWS_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/news"
GOLD_SENTIMENT_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/sentiment"

print(f"🔧 Environment: {ENV}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 FinBERT Sentiment UDF

# COMMAND ----------

# Define return schema for sentiment UDF
SENTIMENT_SCHEMA = StructType([
    StructField("label", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("positive", DoubleType(), True),
    StructField("negative", DoubleType(), True),
    StructField("neutral", DoubleType(), True),
])

@F.udf(returnType=SENTIMENT_SCHEMA)
def analyze_sentiment(text):
    """Analyze sentiment using FinBERT or fallback."""
    if not text or len(text.strip()) < 10:
        return ("neutral", 0.0, 0.33, 0.33, 0.34)

    try:
        from transformers import pipeline
        # Cache the pipeline in a global (reused across rows in partition)
        if not hasattr(analyze_sentiment, "_pipe"):
            analyze_sentiment._pipe = pipeline(
                "sentiment-analysis",
                model="ProsusAI/finbert",
                truncation=True,
                max_length=512,
            )

        result = analyze_sentiment._pipe(text[:512])[0]
        label = result["label"].lower()
        score_val = result["score"]

        # Distribute probabilities
        if label == "positive":
            return (label, score_val, score_val, (1 - score_val) / 2, (1 - score_val) / 2)
        elif label == "negative":
            return (label, -score_val, (1 - score_val) / 2, score_val, (1 - score_val) / 2)
        else:
            return ("neutral", 0.0, (1 - score_val) / 2, (1 - score_val) / 2, score_val)

    except Exception:
        # Fallback: keyword-based sentiment
        text_lower = text.lower()
        pos_words = ["surge", "rally", "bull", "gain", "profit", "soar", "up", "high", "grow", "boom"]
        neg_words = ["crash", "drop", "bear", "loss", "fall", "plunge", "down", "low", "dump", "fear"]
        pos = sum(1 for w in pos_words if w in text_lower)
        neg = sum(1 for w in neg_words if w in text_lower)

        if pos > neg:
            score = min(pos * 0.15, 0.9)
            return ("positive", score, 0.6, 0.2, 0.2)
        elif neg > pos:
            score = -min(neg * 0.15, 0.9)
            return ("negative", score, 0.2, 0.6, 0.2)
        else:
            return ("neutral", 0.0, 0.3, 0.3, 0.4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Process Silver News → Sentiment

# COMMAND ----------

print("─── Loading Silver News ───")
news_df = spark.read.format("delta").load(SILVER_NEWS_PATH)
total_articles = news_df.count()
print(f"✓ Silver news: {total_articles} articles")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Sentiment Analysis

# COMMAND ----------

# Combine title + body for better context
sentiment_input = news_df.withColumn(
    "analysis_text",
    F.concat_ws(". ", F.col("clean_title"), F.col("clean_body"))
)

# Run FinBERT
print("🧠 Running FinBERT sentiment analysis...")
sentiments = (
    sentiment_input
    .withColumn("sentiment", analyze_sentiment(F.col("analysis_text")))
    .select(
        "*",
        F.col("sentiment.label").alias("sentiment_label"),
        F.col("sentiment.score").alias("sentiment_score"),
        F.col("sentiment.positive").alias("prob_positive"),
        F.col("sentiment.negative").alias("prob_negative"),
        F.col("sentiment.neutral").alias("prob_neutral"),
    )
    .drop("sentiment", "analysis_text")
    .withColumn("analyzed_at", F.current_timestamp())
)

print("✓ Sentiment scoring complete")
display(sentiments.select(
    "clean_title", "sentiment_label", "sentiment_score",
    "prob_positive", "prob_negative", "prob_neutral"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Per-Symbol Aggregated Sentiment

# COMMAND ----------

# Explode mentioned_symbols so each article maps to each mentioned symbol
exploded = sentiments.select(
    "*",
    F.explode_outer("mentioned_symbols").alias("symbol")
).filter(F.col("symbol").isNotNull())

# Aggregate per symbol
aggregated = exploded.groupBy("symbol").agg(
    F.count("*").alias("article_count"),
    F.avg("sentiment_score").alias("mean_score"),

    # Confidence-weighted score
    (F.sum(F.col("sentiment_score") * F.greatest(F.col("prob_positive"), F.col("prob_negative"), F.col("prob_neutral")))
     / F.sum(F.greatest(F.col("prob_positive"), F.col("prob_negative"), F.col("prob_neutral")))
    ).alias("weighted_score"),

    F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_count"),
    F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count"),
    F.sum(F.when(F.col("sentiment_label") == "neutral", 1).otherwise(0)).alias("neutral_count"),

    F.avg("prob_positive").alias("avg_prob_positive"),
    F.avg("prob_negative").alias("avg_prob_negative"),
    F.avg("prob_neutral").alias("avg_prob_neutral"),
).withColumn("aggregated_at", F.current_timestamp())

# Dominant label
aggregated = aggregated.withColumn("dominant_label",
    F.when(F.col("positive_count") >= F.col("negative_count"), "positive")
     .when(F.col("negative_count") > F.col("positive_count"), "negative")
     .otherwise("neutral")
)

print("✓ Aggregated sentiment per symbol")
display(aggregated)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Write to Gold Delta

# COMMAND ----------

# Per-article sentiments
(sentiments.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("publish_date", "source")
 .save(f"{GOLD_SENTIMENT_PATH}/articles"))
print(f"✓ Article sentiments → {GOLD_SENTIMENT_PATH}/articles")

# Aggregated per-symbol sentiments
(aggregated.write
 .format("delta")
 .mode("overwrite")
 .save(f"{GOLD_SENTIMENT_PATH}/aggregated"))
print(f"✓ Aggregated sentiment → {GOLD_SENTIMENT_PATH}/aggregated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Sentiment Distribution

# COMMAND ----------

display(
    sentiments.groupBy("sentiment_label").count()
    .orderBy(F.desc("count"))
)
