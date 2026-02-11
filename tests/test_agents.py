"""Tests for agents package."""

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from agents.feature_agent import (
    FeatureAgent,
    compute_rsi,
    compute_ema,
    compute_bollinger_bands,
)
from agents.sentiment_agent import SentimentAgent
from agents.orchestrator import AgentResult, OrchestratedResult


# =============================================================================
# Technical Indicator Tests
# =============================================================================

class TestRSI:
    """Test RSI computation."""

    def test_neutral_with_insufficient_data(self):
        """RSI should return 50 (neutral) with insufficient data."""
        assert compute_rsi([100, 101], period=14) == 50.0

    def test_overbought_rsi(self):
        """RSI should be high with consistent upward movement."""
        prices = [100 + i for i in range(20)]  # steadily rising
        rsi = compute_rsi(prices, period=14)
        assert rsi == 100.0  # all gains, no losses

    def test_oversold_rsi(self):
        """RSI should be low with consistent downward movement."""
        prices = [100 - i for i in range(20)]  # steadily falling
        rsi = compute_rsi(prices, period=14)
        assert rsi == 0.0  # all losses, no gains


class TestEMA:
    """Test EMA computation."""

    def test_empty_values(self):
        """EMA of empty list should be 0."""
        assert compute_ema([], period=20) == 0.0

    def test_single_value(self):
        """EMA of single value should equal that value."""
        assert compute_ema([100.0], period=20) == 100.0

    def test_ema_converges(self):
        """EMA should converge towards recent values."""
        values = [100.0] * 10 + [200.0] * 10
        ema = compute_ema(values, period=5)
        # EMA should be closer to 200 than to 100
        assert ema > 150


class TestBollingerBands:
    """Test Bollinger Bands computation."""

    def test_constant_prices(self):
        """Bollinger Bands with constant prices should have zero bandwidth."""
        prices = [100.0] * 20
        bands = compute_bollinger_bands(prices, period=20)
        assert bands["upper"] == bands["lower"]
        assert bands["bandwidth"] == 0.0

    def test_band_ordering(self):
        """Upper band should always be >= middle >= lower."""
        prices = [100 + (i % 5) for i in range(25)]
        bands = compute_bollinger_bands(prices, period=20)
        assert bands["upper"] >= bands["middle"]
        assert bands["middle"] >= bands["lower"]


# =============================================================================
# Sentiment Agent Tests
# =============================================================================

class TestSentimentAgent:
    """Test SentimentAgent functionality."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Agent should initialize successfully."""
        agent = SentimentAgent()
        await agent.initialize()
        assert agent._initialized is True

    @pytest.mark.asyncio
    async def test_analyze_returns_expected_keys(self):
        """Analyze should return all expected sentiment fields."""
        agent = SentimentAgent()
        await agent.initialize()
        result = await agent.analyze("BTCUSDT")

        expected_keys = [
            "symbol", "sentiment_score", "sentiment_confidence",
            "sentiment_label", "sentiment_momentum", "sentiment_trend",
            "article_count", "news_volume_signal",
        ]
        for key in expected_keys:
            assert key in result, f"Missing key: {key}"

    @pytest.mark.asyncio
    async def test_history_accumulates(self):
        """Sentiment history should grow with repeated calls."""
        agent = SentimentAgent()
        await agent.initialize()

        await agent.analyze("BTCUSDT")
        await agent.analyze("BTCUSDT")
        await agent.analyze("BTCUSDT")

        history = agent._get_or_create_history("BTCUSDT")
        assert len(history) == 3


# =============================================================================
# AgentResult / OrchestratedResult Tests
# =============================================================================

class TestAgentResult:
    """Test AgentResult dataclass."""

    def test_success_result(self):
        result = AgentResult(agent_name="test", data={"key": "val"})
        assert result.success is True
        assert result.error is None

    def test_failure_result(self):
        result = AgentResult(
            agent_name="test",
            success=False,
            error="Something broke",
        )
        assert result.success is False
        assert result.error == "Something broke"


class TestOrchestratedResult:
    """Test OrchestratedResult."""

    def test_all_success(self):
        results = [
            AgentResult(agent_name="a", success=True),
            AgentResult(agent_name="b", success=True),
        ]
        orchestrated = OrchestratedResult(
            symbol="BTCUSDT", agent_results=results
        )
        assert orchestrated.all_success is True

    def test_partial_failure(self):
        results = [
            AgentResult(agent_name="a", success=True),
            AgentResult(agent_name="b", success=False, error="fail"),
        ]
        orchestrated = OrchestratedResult(
            symbol="BTCUSDT", agent_results=results
        )
        assert orchestrated.all_success is False
