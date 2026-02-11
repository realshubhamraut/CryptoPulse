"""
CryptoPulse - API Services

Service layer for API endpoints. Routes requests through the
multi-agent orchestrator for real-time feature computation,
sentiment analysis, and ML inference.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from cryptopulse.config import settings
from cryptopulse.logging import get_api_logger
from cryptopulse.models import (
    PricePrediction,
    MarketFeatures,
    AggregatedSentiment,
    AnomalyAlert,
    PriceDirection,
    SentimentLabel,
    AnomalyType,
)

logger = get_api_logger()


class PredictionService:
    """
    Service for price direction predictions.

    Routes prediction requests through the AgentOrchestrator,
    which fuses market features with sentiment signals before
    running model inference.
    """

    def __init__(self) -> None:
        self.is_ready = False
        self._orchestrator: Any = None
        self._cache: dict[str, PricePrediction] = {}

    async def initialize(self) -> None:
        """Initialize the prediction service and agent orchestrator."""
        try:
            from agents.orchestrator import AgentOrchestrator

            self._orchestrator = AgentOrchestrator()
            await self._orchestrator.initialize()

            self.is_ready = True
            logger.info("prediction_service_initialized", backend="agent_orchestrator")
        except Exception as e:
            logger.error("prediction_service_init_failed", error=str(e))
            self.is_ready = False

    async def get_prediction(
        self,
        symbol: str,
        horizon_minutes: int = 5,
    ) -> PricePrediction | None:
        """
        Get price direction prediction via agent orchestrator.

        Args:
            symbol: Trading pair symbol
            horizon_minutes: Prediction horizon

        Returns:
            PricePrediction or None if not available
        """
        # Check cache (60s TTL)
        cache_key = f"{symbol}:{horizon_minutes}"
        cached = self._cache.get(cache_key)

        if cached and (datetime.now(timezone.utc) - cached.predicted_at).seconds < 60:
            return cached

        if not self._orchestrator:
            logger.warning("orchestrator_not_initialized")
            return None

        try:
            # Run full analysis via orchestrator
            result = await self._orchestrator.analyze(
                symbol=symbol,
                horizon_minutes=horizon_minutes,
            )

            pred = result.prediction

            # Map direction string to enum
            direction_map = {
                "UP": PriceDirection.UP,
                "DOWN": PriceDirection.DOWN,
                "NEUTRAL": PriceDirection.NEUTRAL,
            }

            probs = pred.get("probabilities", {})

            prediction = PricePrediction(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                horizon_minutes=horizon_minutes,
                direction=direction_map.get(pred.get("direction", "NEUTRAL"), PriceDirection.NEUTRAL),
                confidence=pred.get("confidence", 0.5),
                prob_up=probs.get("UP", 0.33),
                prob_down=probs.get("DOWN", 0.33),
                prob_neutral=probs.get("NEUTRAL", 0.34),
                current_price=Decimal(str(result.features.get("close", 0))),
                model_version=pred.get("model_version", "unknown"),
            )

            self._cache[cache_key] = prediction

            logger.info(
                "prediction_served",
                symbol=symbol,
                direction=prediction.direction.value,
                confidence=prediction.confidence,
                latency_ms=result.total_latency_ms,
            )

            return prediction

        except Exception as e:
            logger.error("prediction_error", symbol=symbol, error=str(e))
            return None


class FeatureService:
    """
    Service for market features and sentiment.

    Routes through the FeatureAgent and SentimentAgent
    for real-time data enrichment.
    """

    def __init__(self) -> None:
        self.is_ready = False
        self._orchestrator: Any = None

    async def initialize(self) -> None:
        """Initialize the feature service."""
        try:
            from agents.orchestrator import AgentOrchestrator

            self._orchestrator = AgentOrchestrator()
            await self._orchestrator.initialize()

            self.is_ready = True
            logger.info("feature_service_initialized")
        except Exception as e:
            logger.error("feature_service_init_failed", error=str(e))
            self.is_ready = False

    async def get_latest_features(
        self,
        symbol: str,
        interval: str = "1m",
    ) -> MarketFeatures | None:
        """Get latest market features enriched with technical indicators."""
        if not self._orchestrator:
            return None

        try:
            data = await self._orchestrator.get_features(symbol)

            return MarketFeatures(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                interval=interval,
                open=Decimal(str(data.get("open", 0))),
                high=Decimal(str(data.get("high", 0))),
                low=Decimal(str(data.get("low", 0))),
                close=Decimal(str(data.get("close", 0))),
                volume=Decimal(str(data.get("volume", 0))),
                quote_volume=Decimal(str(data.get("quote_volume", 0))),
                buy_volume=Decimal(str(data.get("buy_volume", 0))),
                sell_volume=Decimal(str(data.get("sell_volume", 0))),
                trade_count=int(data.get("trade_count", 0)),
                vwap=Decimal(str(data.get("vwap", 0))),
                volatility=float(data.get("volatility", 0)),
                trade_intensity=float(data.get("trade_intensity", 0)),
                buy_sell_ratio=float(data.get("buy_sell_ratio", 0.5)),
                price_change_pct=float(data.get("price_change_pct", 0)),
                high_low_range=float(data.get("high_low_range", 0)),
            )

        except Exception as e:
            logger.error("feature_error", symbol=symbol, error=str(e))
            return None

    async def get_latest_sentiment(
        self,
        symbol: str,
    ) -> AggregatedSentiment | None:
        """Get latest aggregated sentiment via SentimentAgent."""
        if not self._orchestrator:
            return None

        try:
            data = await self._orchestrator.get_sentiment(symbol)

            # Map sentiment label
            label_map = {
                "positive": SentimentLabel.POSITIVE,
                "negative": SentimentLabel.NEGATIVE,
                "neutral": SentimentLabel.NEUTRAL,
            }

            now = datetime.now(timezone.utc)

            return AggregatedSentiment(
                symbol=symbol,
                window_start=now - timedelta(minutes=15),
                window_end=now,
                mean_score=data.get("sentiment_score", 0.0),
                weighted_score=data.get("sentiment_score", 0.0),
                article_count=data.get("article_count", 0),
                positive_count=data.get("positive_count", 0),
                negative_count=data.get("negative_count", 0),
                neutral_count=data.get("neutral_count", 0),
                dominant_label=label_map.get(
                    data.get("sentiment_label", "neutral"),
                    SentimentLabel.NEUTRAL,
                ),
            )

        except Exception as e:
            logger.error("sentiment_error", symbol=symbol, error=str(e))
            return None

    async def get_feature_history(
        self,
        symbol: str,
        interval: str,
        limit: int,
    ) -> list[dict]:
        """Get historical features (from Delta Lake)."""
        # TODO: Implement Delta Lake historical query
        return []


class AnomalyService:
    """
    Service for anomaly alerts.

    Routes through the InferenceAgent's anomaly detection
    for real-time market anomaly identification.
    """

    def __init__(self) -> None:
        self.is_ready = False
        self._orchestrator: Any = None

    async def initialize(self) -> None:
        """Initialize the anomaly service."""
        try:
            from agents.orchestrator import AgentOrchestrator

            self._orchestrator = AgentOrchestrator()
            await self._orchestrator.initialize()

            self.is_ready = True
            logger.info("anomaly_service_initialized")
        except Exception as e:
            logger.error("anomaly_service_init_failed", error=str(e))
            self.is_ready = False

    async def get_anomalies(
        self,
        symbol: str | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        severity_min: float = 0.0,
        limit: int = 50,
    ) -> list[AnomalyAlert]:
        """
        Get anomaly alerts via agent orchestrator.
        """
        if not self._orchestrator or not symbol:
            return []

        try:
            result = await self._orchestrator.analyze(
                symbol=symbol,
                include_anomalies=True,
            )

            alerts = []
            for anomaly in result.anomalies:
                severity = anomaly.get("severity", 0.0)
                if severity < severity_min:
                    continue

                # Map anomaly type string to enum
                type_map = {
                    "pump": AnomalyType.PUMP,
                    "dump": AnomalyType.DUMP,
                    "wash_trade": AnomalyType.WASH_TRADE,
                    "volume_spike": AnomalyType.VOLUME_SPIKE,
                    "price_manipulation": AnomalyType.PRICE_MANIPULATION,
                    "unknown": AnomalyType.UNKNOWN,
                }

                alerts.append(AnomalyAlert(
                    symbol=symbol,
                    timestamp=datetime.now(timezone.utc),
                    anomaly_type=type_map.get(
                        anomaly.get("anomaly_type", "unknown"),
                        AnomalyType.UNKNOWN,
                    ),
                    severity=severity,
                    confidence=anomaly.get("confidence", 0.5),
                    description=anomaly.get("description", ""),
                    metrics=anomaly.get("metrics", {}),
                ))

            return alerts[:limit]

        except Exception as e:
            logger.error("anomaly_error", symbol=symbol, error=str(e))
            return []
