"""
Tests for the LangGraph multi-agent system.

Covers agent state, individual agents (with mocked LLM), graph construction,
tools, and orchestrator.
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# =============================================================================
# State Tests
# =============================================================================

class TestAgentState:
    """Test the typed agent state schema."""

    def test_state_can_be_constructed(self):
        from agents.state import AgentState

        state: AgentState = {
            "messages": [],
            "symbol": "BTCUSDT",
            "market_data": {"close": 50000},
            "sentiment_data": {},
            "anomaly_data": [],
            "market_analysis": "",
            "sentiment_analysis": "",
            "risk_assessment": "",
            "final_recommendation": "",
            "confidence_score": 0.0,
            "agent_trace": [],
            "error": "",
        }

        assert state["symbol"] == "BTCUSDT"
        assert state["market_data"]["close"] == 50000
        assert isinstance(state["agent_trace"], list)

    def test_partial_state(self):
        from agents.state import AgentState

        state: AgentState = {
            "symbol": "ETHUSDT",
            "market_data": {},
        }
        assert state["symbol"] == "ETHUSDT"


# =============================================================================
# Tool Tests
# =============================================================================

class TestTools:
    """Test agent tools."""

    def test_tool_list_exists(self):
        from agents.tools import ALL_TOOLS
        assert len(ALL_TOOLS) == 5

    def test_query_gold_features(self):
        from agents.tools import query_gold_features

        result = query_gold_features.invoke("BTCUSDT")
        data = json.loads(result)
        assert data["symbol"] == "BTCUSDT"

    def test_query_sentiment(self):
        from agents.tools import query_sentiment

        result = query_sentiment.invoke("ETHUSDT")
        data = json.loads(result)
        assert data["symbol"] == "ETHUSDT"

    def test_query_anomalies(self):
        from agents.tools import query_anomalies

        result = query_anomalies.invoke("SOLUSDT")
        data = json.loads(result)
        assert data["symbol"] == "SOLUSDT"


# =============================================================================
# Agent Node Tests (with mocked LLM)
# =============================================================================

def _make_base_state():
    """Create a base state for agent tests."""
    from agents.state import AgentState
    return AgentState(
        messages=[],
        symbol="BTCUSDT",
        market_data={
            "close": 50000, "volume": 100, "vwap": 49800,
            "volatility": 0.02, "rsi": 55, "ema_20": 49500,
            "buy_sell_ratio": 0.52, "price_change_pct": 1.5,
            "trade_count": 500, "trade_intensity": 8.3,
            "high_low_range": 2.1,
        },
        sentiment_data={
            "mean_score": 0.35, "weighted_score": 0.4,
            "article_count": 12, "dominant_label": "positive",
        },
        anomaly_data=[],
        market_analysis="",
        sentiment_analysis="",
        risk_assessment="",
        final_recommendation="",
        confidence_score=0.0,
        agent_trace=[],
        error="",
    )


class TestMarketAnalyst:
    """Test MarketAnalyst agent node."""

    @patch("langchain_groq.ChatGroq")
    def test_with_llm(self, mock_llm_class):
        from agents.market_analyst import market_analyst_node

        mock_llm = MagicMock()
        mock_llm.invoke.return_value = MagicMock(content="BULLISH trend confirmed. RSI at 55.")
        mock_llm_class.return_value = mock_llm

        result = market_analyst_node(_make_base_state())
        assert "market_analysis" in result
        assert len(result["market_analysis"]) > 0
        assert "agent_trace" in result

    def test_fallback(self):
        """Test heuristic fallback when LLM fails."""
        from agents.market_analyst import market_analyst_node

        with patch("langchain_groq.ChatGroq") as mock_cls:
            mock_cls.return_value.invoke.side_effect = Exception("No API key")

            result = market_analyst_node(_make_base_state())
            assert "heuristic fallback" in result["market_analysis"]
            assert "MarketAnalyst: ERROR" in result["agent_trace"][0]


class TestSentimentAnalyst:
    """Test SentimentAnalyst agent node."""

    def test_fallback(self):
        from agents.sentiment_analyst import sentiment_analyst_node

        with patch("langchain_groq.ChatGroq") as mock_cls:
            mock_cls.return_value.invoke.side_effect = Exception("No API key")

            result = sentiment_analyst_node(_make_base_state())
            assert "BULLISH sentiment" in result["sentiment_analysis"]


class TestRiskManager:
    """Test RiskManager agent node."""

    def test_high_risk_fallback(self):
        from agents.risk_manager import risk_manager_node

        state = _make_base_state()
        state["anomaly_data"] = [{"type": "volume_spike", "severity": 0.8}]

        with patch("langchain_groq.ChatGroq") as mock_cls:
            mock_cls.return_value.invoke.side_effect = Exception("No API key")

            result = risk_manager_node(state)
            assert "HIGH" in result["risk_assessment"]


class TestPortfolioStrategist:
    """Test PortfolioStrategist agent node."""

    def test_confidence_extraction(self):
        from agents.portfolio_strategist import _extract_confidence

        assert _extract_confidence("Confidence: 75%") == 0.75
        assert _extract_confidence("85% confidence in this BUY") == 0.85
        assert _extract_confidence("Strong buy signal") == 0.85
        assert _extract_confidence("HOLD recommendation") == 0.5


# =============================================================================
# Graph Tests
# =============================================================================

class TestGraph:
    """Test LangGraph StateGraph construction."""

    def test_create_analysis_graph(self):
        from agents.graph import create_analysis_graph
        graph = create_analysis_graph()
        assert graph is not None

    def test_create_routing_graph(self):
        from agents.graph import create_analysis_graph_with_routing
        graph = create_analysis_graph_with_routing()
        assert graph is not None


# =============================================================================
# Orchestrator Tests
# =============================================================================

class TestOrchestrator:
    """Test AgentOrchestrator."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        from agents.orchestrator import AgentOrchestrator
        orch = AgentOrchestrator()
        await orch.initialize()
        assert orch._initialized is True

    def test_direction_extraction(self):
        from agents.orchestrator import _extract_direction
        assert _extract_direction("BUY signal for BTCUSDT") == "UP"
        assert _extract_direction("SELL recommendation") == "DOWN"
        assert _extract_direction("HOLD position") == "NEUTRAL"
        assert _extract_direction("No clear signal") == "NEUTRAL"
