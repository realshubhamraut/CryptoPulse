# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 10 — LangGraph Multi-Agent System
# MAGIC
# MAGIC **CryptoPulse** | Type 5 AI Agent Experimentation
# MAGIC
# MAGIC Interactive notebook for testing the LangGraph-powered multi-agent system.
# MAGIC Four specialist agents collaborate through a StateGraph to produce
# MAGIC actionable trading intelligence.
# MAGIC
# MAGIC | Agent | Role |
# MAGIC |-------|------|
# MAGIC | MarketAnalyst | Technical analysis, trend identification |
# MAGIC | SentimentAnalyst | News sentiment reasoning, narrative themes |
# MAGIC | RiskManager | Anomaly assessment, risk scoring |
# MAGIC | PortfolioStrategist | Signal fusion, trading recommendations |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup

# COMMAND ----------

# MAGIC %pip install langgraph langchain-core langchain-google-genai --quiet

# COMMAND ----------

dbutils.widgets.text("symbol", "BTCUSDT", "Symbol")
dbutils.widgets.text("google_api_key", "", "Google AI API Key")

SYMBOL = dbutils.widgets.get("symbol")
GOOGLE_API_KEY = dbutils.widgets.get("google_api_key")

import os
if GOOGLE_API_KEY:
    os.environ["GOOGLE_API_KEY"] = GOOGLE_API_KEY
else:
    # Try from Databricks secrets
    try:
        os.environ["GOOGLE_API_KEY"] = dbutils.secrets.get(scope="cryptopulse", key="google-api-key")
    except:
        print("⚠ Set GOOGLE_API_KEY widget or add to Databricks secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Load Data Context

# COMMAND ----------

from pyspark.sql import functions as F

ENV = "dev"
STORAGE = f"stcryptopulse{ENV}"

# Load latest features
try:
    features = (
        spark.read.format("delta")
        .load(f"abfss://gold@{STORAGE}.dfs.core.windows.net/features/5m")
        .filter(F.col("symbol") == SYMBOL)
        .orderBy(F.desc("timestamp"))
        .limit(1)
        .toPandas()
    )
    if len(features) > 0:
        MARKET_DATA = features.iloc[0].to_dict()
        print(f"✓ Market data loaded for {SYMBOL}")
    else:
        MARKET_DATA = {"symbol": SYMBOL, "close": 50000, "volume": 100,
                       "volatility": 0.5, "rsi": 55, "buy_sell_ratio": 0.52}
except:
    MARKET_DATA = {"symbol": SYMBOL, "close": 50000, "volume": 100,
                   "volatility": 0.5, "rsi": 55, "buy_sell_ratio": 0.52}
    print("⚠ Using mock market data")

# Load sentiment
try:
    sentiment = (
        spark.read.format("delta")
        .load(f"abfss://gold@{STORAGE}.dfs.core.windows.net/sentiment/aggregated")
        .filter(F.col("symbol") == SYMBOL)
        .toPandas()
    )
    SENTIMENT_DATA = sentiment.iloc[0].to_dict() if len(sentiment) > 0 else {}
except:
    SENTIMENT_DATA = {"mean_score": 0.1, "weighted_score": 0.12,
                      "article_count": 5, "dominant_label": "neutral"}
    print("⚠ Using mock sentiment data")

# Load anomalies
try:
    anomalies = (
        spark.read.format("delta")
        .load(f"abfss://gold@{STORAGE}.dfs.core.windows.net/anomalies")
        .filter(F.col("symbol") == SYMBOL)
        .toPandas()
    )
    ANOMALY_DATA = anomalies.to_dict("records") if len(anomalies) > 0 else []
except:
    ANOMALY_DATA = []
    print("⚠ No anomaly data")

print(f"\n📊 Context loaded:")
print(f"  Market: {len(MARKET_DATA)} fields")
print(f"  Sentiment: {len(SENTIMENT_DATA)} fields")
print(f"  Anomalies: {len(ANOMALY_DATA)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 Create LangGraph Agent System

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/CryptoPulse")

from agents.graph import create_analysis_graph
from agents.state import AgentState

# Create the graph
graph = create_analysis_graph()
print("✓ LangGraph agent graph created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Run Analysis

# COMMAND ----------

# Prepare initial state
initial_state: AgentState = {
    "messages": [],
    "symbol": SYMBOL,
    "market_data": MARKET_DATA,
    "sentiment_data": SENTIMENT_DATA,
    "anomaly_data": ANOMALY_DATA,
    "market_analysis": "",
    "sentiment_analysis": "",
    "risk_assessment": "",
    "final_recommendation": "",
    "confidence_score": 0.0,
    "agent_trace": [],
}

print(f"─── Running LangGraph Analysis for {SYMBOL} ───\n")

# Execute the graph
result = graph.invoke(initial_state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Agent Results

# COMMAND ----------

print("═" * 70)
print(f"  MULTI-AGENT ANALYSIS — {SYMBOL}")
print("═" * 70)

print(f"\n🔹 Market Analyst:")
print(result.get("market_analysis", "N/A"))

print(f"\n🔹 Sentiment Analyst:")
print(result.get("sentiment_analysis", "N/A"))

print(f"\n🔹 Risk Manager:")
print(result.get("risk_assessment", "N/A"))

print(f"\n{'━' * 70}")
print(f"  FINAL RECOMMENDATION")
print(f"{'━' * 70}")
print(result.get("final_recommendation", "N/A"))
print(f"\n  Confidence: {result.get('confidence_score', 0):.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Agent Trace

# COMMAND ----------

trace = result.get("agent_trace", [])
for i, step in enumerate(trace):
    print(f"  [{i+1}] {step}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Custom Query

# COMMAND ----------

# Ask a specific question to the agent system
CUSTOM_QUERY = f"Should I enter a long position on {SYMBOL} right now? What are the risks?"

custom_state: AgentState = {
    **initial_state,
    "messages": [{"role": "user", "content": CUSTOM_QUERY}],
}

custom_result = graph.invoke(custom_state)
print(custom_result.get("final_recommendation", "No recommendation"))
