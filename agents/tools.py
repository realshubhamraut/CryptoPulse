"""
CryptoPulse - Agent Tools

LangChain-compatible tools that agents use to query data sources,
fetch live prices, and run model inference.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx
from langchain_core.tools import tool


# =============================================================================
# Market Data Tools
# =============================================================================

@tool
def fetch_live_price(symbol: str) -> str:
    """
    Fetch the current live price and 24h stats from Binance for a symbol.

    Args:
        symbol: Trading pair symbol, e.g. 'BTCUSDT'

    Returns:
        JSON string with price, volume, and 24h change data
    """
    try:
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        response = httpx.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        return json.dumps({
            "symbol": symbol,
            "price": data["lastPrice"],
            "price_change_24h": data["priceChangePercent"],
            "high_24h": data["highPrice"],
            "low_24h": data["lowPrice"],
            "volume_24h": data["volume"],
            "quote_volume_24h": data["quoteVolume"],
            "trades_24h": data["count"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        return json.dumps({"error": str(e), "symbol": symbol})


@tool
def fetch_order_book(symbol: str, depth: int = 10) -> str:
    """
    Fetch the current order book (bids and asks) for a symbol.

    Args:
        symbol: Trading pair symbol
        depth: Number of price levels to fetch (default 10)

    Returns:
        JSON string with bid/ask levels and spread analysis
    """
    try:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={depth}"
        response = httpx.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        bids = [(float(p), float(q)) for p, q in data["bids"][:5]]
        asks = [(float(p), float(q)) for p, q in data["asks"][:5]]

        best_bid = bids[0][0] if bids else 0
        best_ask = asks[0][0] if asks else 0
        spread = best_ask - best_bid
        spread_pct = (spread / best_bid * 100) if best_bid > 0 else 0

        return json.dumps({
            "symbol": symbol,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": round(spread, 2),
            "spread_pct": round(spread_pct, 6),
            "bid_depth": sum(q for _, q in bids),
            "ask_depth": sum(q for _, q in asks),
            "bid_ask_ratio": round(
                sum(q for _, q in bids) / max(sum(q for _, q in asks), 0.001), 4
            ),
        })
    except Exception as e:
        return json.dumps({"error": str(e), "symbol": symbol})


@tool
def query_gold_features(symbol: str) -> str:
    """
    Query the latest Gold-layer market features from Delta Lake for a symbol.
    Returns OHLCV, VWAP, volatility, RSI, buy/sell ratio, and other indicators.

    Args:
        symbol: Trading pair symbol

    Returns:
        JSON string with latest feature values
    """
    # In Databricks: would read from Delta table
    # Locally: return the data already passed in state (via market_data)
    return json.dumps({
        "note": "Use market_data from state for Gold features",
        "symbol": symbol,
    })


@tool
def query_sentiment(symbol: str) -> str:
    """
    Query the latest aggregated sentiment scores from Gold layer for a symbol.
    Returns mean sentiment, confidence-weighted score, article count, and distribution.

    Args:
        symbol: Trading pair symbol

    Returns:
        JSON string with aggregated sentiment scores
    """
    return json.dumps({
        "note": "Use sentiment_data from state for sentiment",
        "symbol": symbol,
    })


@tool
def query_anomalies(symbol: str) -> str:
    """
    Query detected anomalies for a symbol from the Gold anomaly table.
    Returns recent anomaly alerts with severity and type classification.

    Args:
        symbol: Trading pair symbol

    Returns:
        JSON string with anomaly alerts
    """
    return json.dumps({
        "note": "Use anomaly_data from state for anomalies",
        "symbol": symbol,
    })


# =============================================================================
# All tools list
# =============================================================================

ALL_TOOLS = [
    fetch_live_price,
    fetch_order_book,
    query_gold_features,
    query_sentiment,
    query_anomalies,
]
