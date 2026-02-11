"""
CryptoPulse - Gold Layer Pipelines

Feature engineering and sentiment analysis layer.
"""

from pipelines.gold.feature_engineering import (
    FeatureEngineeringPipeline,
    aggregate_to_higher_interval,
)
from pipelines.gold.sentiment_analysis import (
    SentimentAnalysisPipeline,
    compute_aggregated_sentiment,
)

__all__ = [
    "FeatureEngineeringPipeline",
    "aggregate_to_higher_interval",
    "SentimentAnalysisPipeline", 
    "compute_aggregated_sentiment",
]
