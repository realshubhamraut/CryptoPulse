# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 03 — Silver Layer Pipeline
# MAGIC
# MAGIC **CryptoPulse** | Data cleansing, normalization, and deduplication
# MAGIC
# MAGIC Reads from Bronze Delta tables, applies quality transformations,
# MAGIC and writes cleansed data to Silver Delta tables.
# MAGIC
# MAGIC | Transformation | Description |
# MAGIC |--------------|-------------|
# MAGIC | Type casting | price/quantity → Decimal, timestamps → proper types |
# MAGIC | Deduplication | By trade_id / content_hash |
# MAGIC | Enrichment | Quote quantity, trade side, word count |
# MAGIC | Quality filter | Remove nulls, invalid prices |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")

ENV = dbutils.widgets.get("environment")
STORAGE = f"stcryptopulse{ENV}"

BRONZE_PATH = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"
SILVER_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net"
CHECKPOINT_PATH = f"abfss://checkpoints@{STORAGE}.dfs.core.windows.net/silver"

print(f"🔧 Environment: {ENV}")
print(f"📁 Bronze → Silver")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Silver Trades Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read From Bronze

# COMMAND ----------

trades_bronze = (
    spark.readStream
    .format("delta")
    .load(f"{BRONZE_PATH}/trades")
)

print(f"✓ Reading Bronze trades stream")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Transformations

# COMMAND ----------

trades_silver = (
    trades_bronze
    # --- Type Casting ---
    .withColumn("price", F.col("price").cast("decimal(18,8)"))
    .withColumn("quantity", F.col("quantity").cast("decimal(18,8)"))
    .withColumn("trade_timestamp",
        F.from_unixtime(F.col("trade_time") / 1000).cast("timestamp"))
    .withColumn("event_timestamp",
        F.from_unixtime(F.col("event_time") / 1000).cast("timestamp"))

    # --- Enrichment ---
    .withColumn("quote_quantity",
        F.col("price") * F.col("quantity"))
    .withColumn("trade_side",
        F.when(F.col("is_buyer_maker"), F.lit("SELL")).otherwise(F.lit("BUY")))

    # --- Quality Filters ---
    .filter(F.col("price") > 0)
    .filter(F.col("quantity") > 0)
    .filter(F.col("symbol").isNotNull())
    .filter(F.col("trade_id").isNotNull())

    # --- Deduplication ---
    .withWatermark("trade_timestamp", "1 hour")
    .dropDuplicates(["trade_id", "symbol"])

    # --- Silver Metadata ---
    .withColumn("silver_timestamp", F.current_timestamp())
    .withColumn("trade_date", F.to_date("trade_timestamp"))

    # --- Select Final Columns ---
    .select(
        "trade_id", "symbol", "price", "quantity", "quote_quantity",
        "trade_side", "is_buyer_maker", "trade_timestamp", "event_timestamp",
        "trade_date", "silver_timestamp",
    )
)

print("✓ Silver trade transformations applied")
trades_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Silver Delta

# COMMAND ----------

trades_query = (
    trades_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/trades")
    .partitionBy("trade_date", "symbol")
    .trigger(processingTime="15 seconds")
    .start(f"{SILVER_PATH}/trades")
)

print(f"✓ Silver trades stream → {SILVER_PATH}/trades")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Silver News Pipeline

# COMMAND ----------

import re

# UDF for text cleaning
@F.udf(returnType=StringType())
def clean_text(text):
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)          # Remove HTML
    text = re.sub(r'http\S+', '', text)           # Remove URLs
    text = re.sub(r'\s+', ' ', text).strip()      # Normalize whitespace
    return text

# UDF for extracting crypto mentions
CRYPTO_SYMBOLS = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "DOT", "MATIC", "AVAX",
                  "Bitcoin", "Ethereum", "Solana", "Ripple", "Cardano", "Dogecoin", "Polkadot"}

@F.udf(returnType=ArrayType(StringType()))
def extract_crypto_mentions(text):
    if not text:
        return []
    mentions = set()
    for symbol in CRYPTO_SYMBOLS:
        if symbol.lower() in text.lower():
            mentions.add(symbol.upper() if len(symbol) <= 5 else symbol)
    return list(mentions)

# COMMAND ----------

news_bronze = spark.readStream.format("delta").load(f"{BRONZE_PATH}/news")

news_silver = (
    news_bronze
    # --- Text Cleaning ---
    .withColumn("clean_title", clean_text(F.col("title")))
    .withColumn("clean_body", clean_text(F.col("body")))

    # --- Timestamp Parsing ---
    .withColumn("published_timestamp", F.to_timestamp("published_at"))

    # --- Enrichment ---
    .withColumn("word_count",
        F.size(F.split(F.col("clean_body"), "\\s+")))
    .withColumn("mentioned_symbols",
        extract_crypto_mentions(F.concat(F.col("clean_title"), F.lit(" "), F.col("clean_body"))))

    # --- Quality Filters ---
    .filter(F.col("clean_title") != "")
    .filter(F.col("content_hash").isNotNull())

    # --- Deduplication by content hash ---
    .withWatermark("published_timestamp", "2 hours")
    .dropDuplicates(["content_hash"])

    # --- Silver Metadata ---
    .withColumn("silver_timestamp", F.current_timestamp())
    .withColumn("publish_date", F.to_date("published_timestamp"))

    .select(
        "article_id", "source", "clean_title", "clean_body", "url",
        "published_timestamp", "publish_date", "word_count",
        "mentioned_symbols", "content_hash", "categories",
        "silver_timestamp",
    )
)

news_query = (
    news_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/news")
    .partitionBy("publish_date", "source")
    .trigger(processingTime="30 seconds")
    .start(f"{SILVER_PATH}/news")
)

print(f"✓ Silver news stream → {SILVER_PATH}/news")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Validation

# COMMAND ----------

import time
time.sleep(20)

for table in ["trades", "news"]:
    try:
        df = spark.read.format("delta").load(f"{SILVER_PATH}/{table}")
        count = df.count()
        print(f"✓ Silver {table}: {count} records")
        display(df.limit(5))
    except Exception as e:
        print(f"⚠ Silver {table}: {e}")
