"""Tests for cryptopulse.models (Pydantic schemas)."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from cryptopulse.models import (
    RawTradeEvent,
    CleanedTrade,
    MarketFeatures,
    PricePrediction,
    AnomalyAlert,
    PriceDirection,
    AnomalyType,
)


class TestRawTradeEvent:
    """Test raw trade event validation."""

    def test_valid_trade(self):
        """Valid trade event should parse correctly."""
        event = RawTradeEvent(
            e="trade",
            E=1700000000000,
            s="BTCUSDT",
            t=12345,
            p="50000.50",
            q="0.001",
            b=100,
            a=200,
            T=1700000000000,
            m=False,
            M=True,
        )
        assert event.s == "BTCUSDT"
        assert event.p == "50000.50"

    def test_trade_serialization(self):
        """Trade events should serialize to dict correctly."""
        event = RawTradeEvent(
            e="trade", E=1700000000000, s="ETHUSDT",
            t=1, p="3000.00", q="1.0",
            b=0, a=0, T=1700000000000, m=True, M=True,
        )
        data = event.model_dump()
        assert isinstance(data, dict)
        assert data["s"] == "ETHUSDT"


class TestMarketFeatures:
    """Test market feature model."""

    def test_valid_features(self):
        """MarketFeatures should accept valid data."""
        f = MarketFeatures(
            symbol="BTCUSDT",
            timestamp=datetime.now(timezone.utc),
            interval="1m",
            open=Decimal("50000"),
            high=Decimal("50100"),
            low=Decimal("49900"),
            close=Decimal("50050"),
            volume=Decimal("100"),
            quote_volume=Decimal("5000000"),
            buy_volume=Decimal("55"),
            sell_volume=Decimal("45"),
            trade_count=500,
            vwap=Decimal("50025"),
            volatility=0.5,
            trade_intensity=10.0,
            buy_sell_ratio=0.55,
            price_change_pct=0.1,
            high_low_range=0.4,
        )
        assert f.symbol == "BTCUSDT"
        assert f.buy_sell_ratio == 0.55


class TestPricePrediction:
    """Test prediction model."""

    def test_valid_prediction(self):
        """PricePrediction should accept valid data."""
        p = PricePrediction(
            symbol="BTCUSDT",
            timestamp=datetime.now(timezone.utc),
            horizon_minutes=5,
            direction=PriceDirection.UP,
            confidence=0.85,
            prob_up=0.85,
            prob_down=0.10,
            prob_neutral=0.05,
            current_price=Decimal("50000"),
            model_version="v1.0",
        )
        assert p.direction == PriceDirection.UP
        assert p.confidence == 0.85

    def test_direction_enum_values(self):
        """PriceDirection should have correct enum values."""
        assert PriceDirection.UP.value == "up"
        assert PriceDirection.DOWN.value == "down"
        assert PriceDirection.NEUTRAL.value == "neutral"


class TestAnomalyAlert:
    """Test anomaly alert model."""

    def test_valid_anomaly(self):
        """AnomalyAlert should accept valid data."""
        a = AnomalyAlert(
            symbol="BTCUSDT",
            timestamp=datetime.now(timezone.utc),
            anomaly_type=AnomalyType.VOLUME_SPIKE,
            severity=0.8,
            confidence=0.9,
            description="Unusual volume detected",
            metrics={"volume_zscore": 3.5},
        )
        assert a.anomaly_type == AnomalyType.VOLUME_SPIKE
        assert a.severity == 0.8
