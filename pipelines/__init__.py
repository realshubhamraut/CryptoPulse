"""
CryptoPulse - Streaming Pipelines

Medallion architecture pipeline implementations:
- Bronze: Raw data ingestion
- Silver: Cleansed and normalized data
- Gold: Features and analytics
"""

from pipelines.bronze import BronzeTradesPipeline, BronzeNewsPipeline
from pipelines.silver import SilverTradesPipeline, SilverNewsPipeline
from pipelines.gold import FeatureEngineeringPipeline, SentimentAnalysisPipeline

__all__ = [
    # Bronze
    "BronzeTradesPipeline",
    "BronzeNewsPipeline",
    # Silver
    "SilverTradesPipeline",
    "SilverNewsPipeline",
    # Gold
    "FeatureEngineeringPipeline",
    "SentimentAnalysisPipeline",
]
