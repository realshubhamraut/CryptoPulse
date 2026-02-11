# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 02 — Bronze Layer Pipeline
# MAGIC
# MAGIC **CryptoPulse** | Raw data ingestion into Delta Lake Bronze tables
# MAGIC
# MAGIC Reads streaming data from Azure Event Hubs and writes to Bronze Delta tables
# MAGIC with schema enforcement. Preserves raw data exactly as received.
# MAGIC
# MAGIC | Parameter | Value |
# MAGIC |-----------|-------|
# MAGIC | Source | Azure Event Hubs (trades, news) |
# MAGIC | Sink | Delta Lake Bronze tables |
# MAGIC | Mode | Spark Structured Streaming |
# MAGIC | Partitioning | date, symbol/source |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.dropdown("stream_type", "trades", ["trades", "news", "both"], "Stream Type")

ENV = dbutils.widgets.get("environment")
STREAM_TYPE = dbutils.widgets.get("stream_type")

# Paths
BRONZE_PATH = f"abfss://bronze@stcryptopulse{ENV}.dfs.core.windows.net"
CHECKPOINT_PATH = f"abfss://checkpoints@stcryptopulse{ENV}.dfs.core.windows.net/bronze"

# Event Hub config
EVENTHUB_CONN = dbutils.secrets.get(scope="cryptopulse", key="eventhub-connection-string")

print(f"🔧 Environment: {ENV}")
print(f"📁 Bronze Path: {BRONZE_PATH}")
print(f"📡 Stream Type: {STREAM_TYPE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports & Schemas

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

# Raw trade event schema
TRADE_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_time", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("trade_id", LongType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("trade_time", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("ingested_at", StringType(), True),
])

# Raw news event schema
NEWS_SCHEMA = StructType([
    StructField("article_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True),
    StructField("url", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("categories", StringType(), True),
    StructField("ingested_at", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Trade Streaming Pipeline (Event Hub → Bronze Delta)

# COMMAND ----------

def create_eventhub_config(hub_name: str) -> dict:
    """Create Event Hub Spark config."""
    conn_string = f"{EVENTHUB_CONN};EntityPath={hub_name}"
    return {
        "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_string),
        "eventhubs.consumerGroup": "$Default",
        "eventhubs.startingPosition": json.dumps({
            "offset": "-1",
            "seqNo": -1,
            "enqueuedTime": None,
            "isInclusive": True,
        }),
    }

# COMMAND ----------

if STREAM_TYPE in ("trades", "both"):
    print("─── Starting Trade Bronze Stream ───")

    eh_conf = create_eventhub_config("trades")

    trades_stream = (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
        .select(
            F.from_json(F.col("body").cast("string"), TRADE_SCHEMA).alias("data"),
            F.col("enqueuedTime").alias("enqueued_at"),
            F.col("offset"),
            F.col("sequenceNumber").alias("sequence_number"),
        )
        .select("data.*", "enqueued_at", "offset", "sequence_number")
        # Add Bronze metadata
        .withColumn("bronze_timestamp", F.current_timestamp())
        .withColumn("bronze_date", F.to_date(F.from_unixtime(F.col("event_time") / 1000)))
    )

    # Write to Delta
    trades_query = (
        trades_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/trades")
        .partitionBy("bronze_date", "symbol")
        .trigger(processingTime="10 seconds")
        .start(f"{BRONZE_PATH}/trades")
    )

    print(f"✓ Trade stream started → {BRONZE_PATH}/trades")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 News Streaming Pipeline (Event Hub → Bronze Delta)

# COMMAND ----------

if STREAM_TYPE in ("news", "both"):
    print("─── Starting News Bronze Stream ───")

    eh_conf = create_eventhub_config("news")

    news_stream = (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
        .select(
            F.from_json(F.col("body").cast("string"), NEWS_SCHEMA).alias("data"),
            F.col("enqueuedTime").alias("enqueued_at"),
        )
        .select("data.*", "enqueued_at")
        .withColumn("bronze_timestamp", F.current_timestamp())
        .withColumn("publish_date", F.to_date(F.col("published_at")))
    )

    news_query = (
        news_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/news")
        .partitionBy("publish_date", "source")
        .trigger(processingTime="30 seconds")
        .start(f"{BRONZE_PATH}/news")
    )

    print(f"✓ News stream started → {BRONZE_PATH}/news")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Validation — Verify Bronze Tables

# COMMAND ----------

import time
time.sleep(15)  # Wait for some data to arrive

# Check trades Bronze
try:
    trades_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/trades")
    trade_count = trades_bronze.count()
    print(f"✓ Bronze trades: {trade_count} records")
    display(trades_bronze.orderBy(F.desc("bronze_timestamp")).limit(5))
except Exception as e:
    print(f"⚠ Bronze trades not yet available: {e}")

# Check news Bronze
try:
    news_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/news")
    news_count = news_bronze.count()
    print(f"✓ Bronze news: {news_count} records")
    display(news_bronze.orderBy(F.desc("bronze_timestamp")).limit(5))
except Exception as e:
    print(f"⚠ Bronze news not yet available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Bronze Layer Summary
# MAGIC
# MAGIC | Table | Schema | Partitioning |
# MAGIC |-------|--------|-------------|
# MAGIC | `bronze/trades` | trade_id, symbol, price, quantity, ... | date, symbol |
# MAGIC | `bronze/news` | article_id, source, title, body, ... | publish_date, source |
