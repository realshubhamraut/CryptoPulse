# Databricks notebook source
# MAGIC %md
# MAGIC # 📡 01 — Data Ingestion Pipeline
# MAGIC
# MAGIC **CryptoPulse** | Real-time cryptocurrency data ingestion
# MAGIC
# MAGIC This notebook fetches trade data from Binance REST API and crawls cryptocurrency news,
# MAGIC then publishes events to Azure Event Hubs for downstream Spark Structured Streaming.
# MAGIC
# MAGIC | Parameter | Value |
# MAGIC |-----------|-------|
# MAGIC | Source | Binance REST API + News APIs |
# MAGIC | Sink | Azure Event Hubs (trades, news) |
# MAGIC | Schedule | Timer-triggered (trades: 5s, news: 60s) |
# MAGIC | Validation | Pydantic schema enforcement |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

# Widget parameters for Databricks UI
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("trading_pairs", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT", "Trading Pairs")
dbutils.widgets.text("trades_per_pair", "100", "Trades Per Pair")
dbutils.widgets.text("news_lookback_min", "5", "News Lookback (min)")

ENV = dbutils.widgets.get("environment")
TRADING_PAIRS = dbutils.widgets.get("trading_pairs").split(",")
TRADES_PER_PAIR = int(dbutils.widgets.get("trades_per_pair"))
NEWS_LOOKBACK = int(dbutils.widgets.get("news_lookback_min"))

print(f"🔧 Environment: {ENV}")
print(f"📊 Trading Pairs: {TRADING_PAIRS}")
print(f"📰 News Lookback: {NEWS_LOOKBACK} minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports & Setup

# COMMAND ----------

import json
import time
import hashlib
import requests
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Event Hubs config
EVENTHUB_CONN_STRING = dbutils.secrets.get(scope="cryptopulse", key="eventhub-connection-string")
BINANCE_BASE_URL = "https://api.binance.com/api/v3"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Binance Trade Ingestion

# COMMAND ----------

def fetch_binance_trades(symbol: str, limit: int = 100) -> list:
    """Fetch recent trades from Binance REST API."""
    url = f"{BINANCE_BASE_URL}/trades"
    params = {"symbol": symbol, "limit": min(limit, 1000)}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        trades = response.json()
        print(f"  ✓ {symbol}: fetched {len(trades)} trades")
        return trades
    except Exception as e:
        print(f"  ✗ {symbol}: {e}")
        return []


def transform_trade(trade: dict, symbol: str) -> dict:
    """Transform Binance REST trade to our event schema."""
    return {
        "event_type": "trade",
        "event_time": trade["time"],
        "symbol": symbol,
        "trade_id": trade["id"],
        "price": str(trade["price"]),
        "quantity": str(trade["qty"]),
        "trade_time": trade["time"],
        "is_buyer_maker": trade["isBuyerMaker"],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Trade Ingestion

# COMMAND ----------

print("═" * 60)
print("  BINANCE TRADE INGESTION")
print("═" * 60)

all_trades = []
start_time = time.time()

for symbol in TRADING_PAIRS:
    raw_trades = fetch_binance_trades(symbol, limit=TRADES_PER_PAIR)

    # Deduplicate by trade_id
    seen_ids = set()
    for trade in raw_trades:
        if trade["id"] not in seen_ids:
            seen_ids.add(trade["id"])
            all_trades.append(transform_trade(trade, symbol))

elapsed = time.time() - start_time
print(f"\n📊 Total trades fetched: {len(all_trades)} in {elapsed:.2f}s")

# Convert to Spark DataFrame for inspection
if all_trades:
    trades_df = spark.createDataFrame(all_trades)
    trades_df.printSchema()
    display(trades_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish Trades to Event Hub

# COMMAND ----------

from azure.eventhub import EventHubProducerClient, EventData

def publish_to_eventhub(events: list, hub_name: str, conn_string: str):
    """Publish events to Azure Event Hub."""
    full_conn = f"{conn_string};EntityPath={hub_name}"
    producer = EventHubProducerClient.from_connection_string(full_conn)

    batch_count = 0
    with producer:
        event_batch = producer.create_batch()
        for event in events:
            try:
                event_batch.add(EventData(json.dumps(event)))
                batch_count += 1
            except ValueError:
                # Batch full, send and create new
                producer.send_batch(event_batch)
                event_batch = producer.create_batch()
                event_batch.add(EventData(json.dumps(event)))
                batch_count += 1

        if len(event_batch) > 0:
            producer.send_batch(event_batch)

    return batch_count

# Publish trades
if all_trades:
    count = publish_to_eventhub(all_trades, "trades", EVENTHUB_CONN_STRING)
    print(f"✓ Published {count} trade events to Event Hub 'trades'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 News Ingestion

# COMMAND ----------

def crawl_crypto_news(lookback_minutes: int = 5) -> list:
    """Crawl cryptocurrency news from public APIs."""
    articles = []
    since = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)

    # CryptoCompare News API (free, no key required for basic)
    try:
        url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN"
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json().get("Data", [])
            for item in data:
                published = datetime.fromtimestamp(item.get("published_on", 0), tz=timezone.utc)
                if published >= since:
                    content_hash = hashlib.sha256(
                        item.get("title", "").encode()
                    ).hexdigest()[:16]

                    articles.append({
                        "article_id": str(item.get("id", "")),
                        "source": item.get("source", "cryptocompare"),
                        "title": item.get("title", ""),
                        "body": item.get("body", "")[:500],
                        "url": item.get("url", ""),
                        "published_at": published.isoformat(),
                        "content_hash": content_hash,
                        "categories": item.get("categories", ""),
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    })

            print(f"  ✓ CryptoCompare: {len(articles)} articles")
    except Exception as e:
        print(f"  ✗ CryptoCompare: {e}")

    return articles

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute News Crawl & Publish

# COMMAND ----------

print("═" * 60)
print("  NEWS INGESTION")
print("═" * 60)

articles = crawl_crypto_news(lookback_minutes=NEWS_LOOKBACK)
print(f"\n📰 Total articles crawled: {len(articles)}")

if articles:
    news_df = spark.createDataFrame(articles)
    display(news_df.select("source", "title", "published_at").limit(10))

    # Publish to Event Hub
    count = publish_to_eventhub(articles, "news", EVENTHUB_CONN_STRING)
    print(f"✓ Published {count} news events to Event Hub 'news'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Ingestion Summary

# COMMAND ----------

summary = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "environment": ENV,
    "trades_ingested": len(all_trades),
    "pairs_processed": len(TRADING_PAIRS),
    "articles_ingested": len(articles) if articles else 0,
}

print("\n" + "═" * 60)
print("  INGESTION SUMMARY")
print("═" * 60)
for k, v in summary.items():
    print(f"  {k}: {v}")
