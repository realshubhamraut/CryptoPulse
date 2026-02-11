"""
CryptoPulse - Data Models

Pydantic models for all data entities in the system.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class TradeSide(str, Enum):
    """Trade side (buyer is maker or not)."""
    
    BUY = "buy"
    SELL = "sell"


class SentimentLabel(str, Enum):
    """Sentiment classification labels."""
    
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class PriceDirection(str, Enum):
    """Price direction prediction labels."""
    
    UP = "up"
    DOWN = "down"
    NEUTRAL = "neutral"


class AnomalyType(str, Enum):
    """Types of detected anomalies."""
    
    PUMP = "pump"
    DUMP = "dump"
    WASH_TRADE = "wash_trade"
    VOLUME_SPIKE = "volume_spike"
    PRICE_MANIPULATION = "price_manipulation"
    UNKNOWN = "unknown"


# =============================================================================
# Raw Event Models (Bronze Layer)
# =============================================================================

class RawTradeEvent(BaseModel):
    """
    Raw trade event from Binance WebSocket stream.
    
    Maps to Binance Trade Stream format:
    https://binance-docs.github.io/apidocs/spot/en/#trade-streams
    """
    
    event_type: str = Field(alias="e", default="trade")
    event_time: int = Field(alias="E", description="Event time in milliseconds")
    symbol: str = Field(alias="s", description="Trading pair symbol")
    trade_id: int = Field(alias="t", description="Trade ID")
    price: str = Field(alias="p", description="Trade price")
    quantity: str = Field(alias="q", description="Trade quantity")
    buyer_order_id: int = Field(alias="b", description="Buyer order ID")
    seller_order_id: int = Field(alias="a", description="Seller order ID")
    trade_time: int = Field(alias="T", description="Trade time in milliseconds")
    is_buyer_maker: bool = Field(alias="m", description="Is buyer the maker")
    ignore: bool = Field(alias="M", default=True)

    model_config = {"populate_by_name": True}


class RawNewsEvent(BaseModel):
    """Raw news article event from crawlers."""
    
    source: str = Field(description="News source (e.g., 'coindesk')")
    article_id: str = Field(description="Unique article identifier")
    title: str = Field(description="Article title")
    content: str = Field(description="Full article content")
    url: str = Field(description="Article URL")
    published_at: datetime = Field(description="Publication timestamp")
    crawled_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Crawl timestamp",
    )
    author: str | None = Field(default=None, description="Article author")
    tags: list[str] = Field(default_factory=list, description="Article tags")
    content_hash: str = Field(description="Hash for deduplication")
    
    @field_validator("content_hash", mode="before")
    @classmethod
    def generate_hash(cls, v: str, info: Any) -> str:
        """Generate content hash if not provided."""
        if v:
            return v
        import xxhash
        content = info.data.get("content", "")
        title = info.data.get("title", "")
        return xxhash.xxh64(f"{title}:{content}").hexdigest()


# =============================================================================
# Cleaned Models (Silver Layer)
# =============================================================================

class CleanTrade(BaseModel):
    """Cleaned and normalized trade data."""
    
    trade_id: int = Field(description="Unique trade identifier")
    symbol: str = Field(description="Trading pair (e.g., BTCUSDT)")
    price: Decimal = Field(description="Trade price")
    quantity: Decimal = Field(description="Trade quantity")
    quote_quantity: Decimal = Field(description="Quote asset quantity (price * qty)")
    side: TradeSide = Field(description="Trade side")
    timestamp: datetime = Field(description="Trade timestamp (UTC)")
    event_timestamp: datetime = Field(description="Event receipt timestamp (UTC)")
    
    @property
    def is_buy(self) -> bool:
        return self.side == TradeSide.BUY


class CleanNews(BaseModel):
    """Cleaned and normalized news article."""
    
    article_id: str = Field(description="Unique article identifier")
    source: str = Field(description="News source")
    title: str = Field(description="Cleaned title")
    content: str = Field(description="Cleaned content")
    url: str = Field(description="Article URL")
    published_at: datetime = Field(description="Publication timestamp (UTC)")
    crawled_at: datetime = Field(description="Crawl timestamp (UTC)")
    mentioned_symbols: list[str] = Field(
        default_factory=list,
        description="Cryptocurrency symbols mentioned",
    )
    word_count: int = Field(description="Content word count")
    content_hash: str = Field(description="Content hash for deduplication")


# =============================================================================
# Feature Models (Gold Layer)
# =============================================================================

class OHLCCandle(BaseModel):
    """OHLC candlestick data."""
    
    symbol: str
    interval: str = Field(description="Interval (e.g., '1m', '5m', '15m')")
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    
    @property
    def body_pct(self) -> float:
        """Price change percentage."""
        if self.open == 0:
            return 0.0
        return float((self.close - self.open) / self.open * 100)


class MarketFeatures(BaseModel):
    """Computed market microstructure features for a symbol."""
    
    symbol: str
    timestamp: datetime
    interval: str = Field(description="Feature computation interval")
    
    # OHLC
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    
    # Volume metrics
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    buy_volume: Decimal
    sell_volume: Decimal
    
    # VWAP
    vwap: Decimal = Field(description="Volume Weighted Average Price")
    
    # Volatility
    volatility: float = Field(description="Rolling standard deviation of returns")
    
    # Order flow
    trade_intensity: float = Field(description="Trades per second")
    buy_sell_ratio: float = Field(description="Buy volume / Total volume")
    
    # Price metrics
    price_change_pct: float = Field(description="Percentage price change")
    high_low_range: float = Field(description="(High - Low) / Low * 100")


class SentimentScore(BaseModel):
    """Sentiment analysis result for news article."""
    
    article_id: str
    symbol: str | None = Field(description="Related symbol if identifiable")
    label: SentimentLabel
    score: float = Field(ge=-1.0, le=1.0, description="Sentiment score [-1, 1]")
    confidence: float = Field(ge=0.0, le=1.0, description="Model confidence")
    timestamp: datetime
    source: str


class AggregatedSentiment(BaseModel):
    """Aggregated sentiment for a symbol over a time window."""
    
    symbol: str
    window_start: datetime
    window_end: datetime
    
    # Aggregated scores
    mean_score: float
    weighted_score: float = Field(description="Confidence-weighted mean")
    article_count: int
    
    # Distribution
    positive_count: int
    negative_count: int
    neutral_count: int
    
    # Dominant sentiment
    dominant_label: SentimentLabel


# =============================================================================
# Prediction Models
# =============================================================================

class PricePrediction(BaseModel):
    """Price direction prediction."""
    
    symbol: str
    timestamp: datetime
    horizon_minutes: int = Field(description="Prediction horizon in minutes")
    
    # Prediction
    direction: PriceDirection
    confidence: float = Field(ge=0.0, le=1.0)
    
    # Probabilities
    prob_up: float
    prob_down: float
    prob_neutral: float
    
    # Context
    current_price: Decimal
    predicted_at: datetime = Field(default_factory=datetime.utcnow)
    model_version: str = Field(default="v1.0.0")


class AnomalyAlert(BaseModel):
    """Detected trading anomaly."""
    
    symbol: str
    timestamp: datetime
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Anomaly details
    anomaly_type: AnomalyType
    severity: float = Field(ge=0.0, le=1.0, description="Anomaly severity score")
    confidence: float = Field(ge=0.0, le=1.0)
    
    # Context
    description: str
    metrics: dict[str, float] = Field(
        default_factory=dict,
        description="Relevant metrics that triggered the alert",
    )
    
    # Status
    acknowledged: bool = Field(default=False)
    false_positive: bool | None = Field(default=None)


# =============================================================================
# API Response Models
# =============================================================================

class HealthStatus(BaseModel):
    """API health check response."""
    
    status: str = "healthy"
    version: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    services: dict[str, bool] = Field(default_factory=dict)


class PredictionResponse(BaseModel):
    """API response for prediction endpoint."""
    
    symbol: str
    prediction: PricePrediction
    features: MarketFeatures | None = None
    sentiment: AggregatedSentiment | None = None


class AnomalyListResponse(BaseModel):
    """API response for anomaly list endpoint."""
    
    count: int
    anomalies: list[AnomalyAlert]
    from_timestamp: datetime
    to_timestamp: datetime
