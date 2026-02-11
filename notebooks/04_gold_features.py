# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 04 — Gold Layer: Feature Engineering
# MAGIC
# MAGIC **CryptoPulse** | Market microstructure feature computation
# MAGIC
# MAGIC Computes technical indicators and market features from Silver trade data,
# MAGIC writing enriched feature vectors to Gold Delta tables.
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | OHLC | Open/High/Low/Close candlesticks |
# MAGIC | VWAP | Volume-weighted average price |
# MAGIC | Volatility | Price standard deviation |
# MAGIC | RSI / EMA | Relative Strength Index, Exponential Moving Average |
# MAGIC | Bollinger | Upper/Middle/Lower bands |
# MAGIC | Microstructure | Trade intensity, buy/sell ratio, bid-ask imbalance |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.dropdown("mode", "batch", ["batch", "stream"], "Processing Mode")
dbutils.widgets.text("intervals", "1m,5m,15m", "Aggregation Intervals")

ENV = dbutils.widgets.get("environment")
MODE = dbutils.widgets.get("mode")
INTERVALS = dbutils.widgets.get("intervals").split(",")

STORAGE = f"stcryptopulse{ENV}"
SILVER_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/trades"
GOLD_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/features"
CHECKPOINT_PATH = f"abfss://checkpoints@{STORAGE}.dfs.core.windows.net/gold"

print(f"🔧 Mode: {MODE} | Intervals: {INTERVALS}")

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📐 Feature Functions

# COMMAND ----------

def compute_ohlcv(df, interval_col="window"):
    """Compute OHLC + Volume aggregates."""
    return df.groupBy("symbol", interval_col).agg(
        F.first("price").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.last("price").alias("close"),
        F.sum("quantity").alias("volume"),
        F.sum("quote_quantity").alias("quote_volume"),
        F.count("*").alias("trade_count"),
        F.sum(F.when(F.col("trade_side") == "BUY", F.col("quantity")).otherwise(0)).alias("buy_volume"),
        F.sum(F.when(F.col("trade_side") == "SELL", F.col("quantity")).otherwise(0)).alias("sell_volume"),
    )


def compute_vwap(df):
    """Volume-Weighted Average Price."""
    return df.withColumn("vwap",
        F.col("quote_volume") / F.when(F.col("volume") > 0, F.col("volume")).otherwise(1))


def compute_volatility(df):
    """Price volatility as (high - low) / close."""
    return df.withColumn("volatility",
        (F.col("high") - F.col("low")) / F.when(F.col("close") > 0, F.col("close")).otherwise(1))


def compute_trade_intensity(df):
    """Trades per second within the interval."""
    return df.withColumn("trade_intensity",
        F.col("trade_count") / 60.0)  # per minute


def compute_buy_sell_ratio(df):
    """Buy/sell volume ratio."""
    total = F.col("buy_volume") + F.col("sell_volume")
    return df.withColumn("buy_sell_ratio",
        F.when(total > 0, F.col("buy_volume") / total).otherwise(0.5))


def compute_price_change(df):
    """Price change percentage."""
    return df.withColumn("price_change_pct",
        F.when(F.col("open") > 0,
               ((F.col("close") - F.col("open")) / F.col("open")) * 100
        ).otherwise(0))


def compute_high_low_range(df):
    """High-low range as percentage of close."""
    return df.withColumn("high_low_range",
        F.when(F.col("close") > 0,
               ((F.col("high") - F.col("low")) / F.col("close")) * 100
        ).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC ### RSI / EMA / Bollinger (UDFs)

# COMMAND ----------

@F.udf(returnType=DoubleType())
def compute_rsi_udf(prices_json):
    """Compute RSI from JSON array of prices."""
    import json
    if not prices_json:
        return 50.0
    prices = json.loads(prices_json)
    if len(prices) < 15:
        return 50.0

    gains, losses = [], []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains[-14:]) / 14
    avg_loss = sum(losses[-14:]) / 14

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


@F.udf(returnType=DoubleType())
def compute_ema_udf(prices_json, period):
    """Compute EMA from JSON array of prices."""
    import json
    if not prices_json:
        return 0.0
    prices = json.loads(prices_json)
    if not prices:
        return 0.0

    multiplier = 2.0 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = (p - ema) * multiplier + ema
    return float(ema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Feature Pipeline Execution

# COMMAND ----------

if MODE == "batch":
    # Batch mode: process all Silver data
    silver_trades = spark.read.format("delta").load(SILVER_PATH)
    print(f"✓ Loaded Silver trades: {silver_trades.count()} records")
else:
    # Stream mode
    silver_trades = spark.readStream.format("delta").load(SILVER_PATH)
    print("✓ Silver trades stream opened")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compute Features per Interval

# COMMAND ----------

from pyspark.sql.functions import window

for interval in INTERVALS:
    print(f"\n─── Computing features for interval: {interval} ───")

    # Parse interval to Spark window duration
    interval_map = {"1m": "1 minute", "5m": "5 minutes", "15m": "15 minutes",
                    "1h": "1 hour", "4h": "4 hours", "1d": "1 day"}
    spark_interval = interval_map.get(interval, "5 minutes")

    if MODE == "batch":
        # Batch: window aggregation
        windowed = silver_trades.groupBy(
            "symbol",
            window("trade_timestamp", spark_interval),
        ).agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.sum("quote_quantity").alias("quote_volume"),
            F.count("*").alias("trade_count"),
            F.sum(F.when(F.col("trade_side") == "BUY", F.col("quantity")).otherwise(0)).alias("buy_volume"),
            F.sum(F.when(F.col("trade_side") == "SELL", F.col("quantity")).otherwise(0)).alias("sell_volume"),
            F.collect_list("price").cast("string").alias("price_history"),
        )

        # Flatten window column
        features = (
            windowed
            .withColumn("timestamp", F.col("window.start"))
            .withColumn("interval", F.lit(interval))
            .drop("window")
        )

        # Apply feature functions
        features = compute_vwap(features)
        features = compute_volatility(features)
        features = compute_trade_intensity(features)
        features = compute_buy_sell_ratio(features)
        features = compute_price_change(features)
        features = compute_high_low_range(features)

        # Technical indicators from price history
        features = features.withColumn("rsi", compute_rsi_udf(F.col("price_history")))
        features = features.withColumn("ema_20", compute_ema_udf(F.col("price_history"), F.lit(20)))
        features = features.withColumn("feature_date", F.to_date("timestamp"))

        # Drop intermediate columns
        features = features.drop("price_history")

        # Write to Gold Delta
        (features.write
         .format("delta")
         .mode("overwrite")
         .partitionBy("feature_date", "symbol")
         .save(f"{GOLD_PATH}/{interval}"))

        print(f"  ✓ Written {features.count()} feature rows → {GOLD_PATH}/{interval}")
        display(features.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Feature Summary

# COMMAND ----------

if MODE == "batch":
    for interval in INTERVALS:
        try:
            df = spark.read.format("delta").load(f"{GOLD_PATH}/{interval}")
            print(f"\n{interval} features: {df.count()} rows, {len(df.columns)} columns")
            df.describe("close", "volume", "vwap", "volatility", "rsi").show()
        except:
            pass
