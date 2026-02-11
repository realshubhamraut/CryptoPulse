"""
CryptoPulse - Ingestion Module

Real-time data ingestion from Binance and news sources.
"""

from cryptopulse.ingestion.binance_client import (
    BinanceWebSocketClient,
    BinanceOrderBookClient,
    run_trade_ingestion,
)
from cryptopulse.ingestion.kafka_producer import (
    EventPublisher,
    TradeEventPublisher,
    NewsEventPublisher,
)
from cryptopulse.ingestion.news_crawler import (
    BaseCrawler,
    NewsArticle,
    NewsCrawlerOrchestrator,
    create_default_orchestrator,
)

__all__ = [
    # Binance
    "BinanceWebSocketClient",
    "BinanceOrderBookClient", 
    "run_trade_ingestion",
    # Kafka
    "EventPublisher",
    "TradeEventPublisher",
    "NewsEventPublisher",
    # News
    "BaseCrawler",
    "NewsArticle",
    "NewsCrawlerOrchestrator",
    "create_default_orchestrator",
]
