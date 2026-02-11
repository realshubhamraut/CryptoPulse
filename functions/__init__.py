"""
CryptoPulse - Azure Functions Ingestion Package

Serverless event-driven ingestion for:
- Binance trade data (timer-triggered, every 5s)
- News articles (timer-triggered, every 60s)
"""

from functions.binance_ingestion import ingest_trades
from functions.news_ingestion import ingest_news

__all__ = ["ingest_trades", "ingest_news"]
