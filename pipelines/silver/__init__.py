"""
CryptoPulse - Silver Layer Pipelines

Cleansed and normalized data layer.
"""

from pipelines.silver.trades_silver import SilverTradesPipeline
from pipelines.silver.news_silver import SilverNewsPipeline

__all__ = [
    "SilverTradesPipeline",
    "SilverNewsPipeline",
]
