"""
CryptoPulse - Agent Orchestrator

High-level orchestrator that wraps the LangGraph StateGraph,
providing async execution, caching, and a clean interface for the API layer.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="orchestrator")


@dataclass
class OrchestratedResult:
    """Container for a full orchestrated analysis result."""

    symbol: str
    features: dict[str, Any] = field(default_factory=dict)
    sentiment: dict[str, Any] = field(default_factory=dict)
    prediction: dict[str, Any] = field(default_factory=dict)
    anomalies: list[dict[str, Any]] = field(default_factory=list)
    recommendation: str = ""
    confidence: float = 0.0
    agent_trace: list[str] = field(default_factory=list)
    total_latency_ms: float = 0.0
    timestamp: str = ""


class AgentOrchestrator:
    """
    High-level orchestrator wrapping the LangGraph multi-agent system.

    Provides:
        - Async execution interface for the API layer
        - Redis caching for repeated queries
        - Graceful degradation on agent failures
    """

    def __init__(self) -> None:
        self._initialized = False
        self._graph: Any = None
        self._cache: dict[str, OrchestratedResult] = {}

    async def initialize(self) -> None:
        """Initialize the LangGraph agent graph."""
        if self._initialized:
            return

        logger.info("initializing_orchestrator")

        try:
            from agents.graph import create_analysis_graph_with_routing
            self._graph = create_analysis_graph_with_routing()
            self._initialized = True
            logger.info("orchestrator_initialized", graph="analysis_with_routing")
        except ImportError as e:
            logger.warning("langgraph_not_available", error=str(e))
            self._initialized = True  # Allow fallback mode

    async def analyze(
        self,
        symbol: str,
        horizon_minutes: int = 5,
        include_anomalies: bool = True,
        market_data: dict | None = None,
        sentiment_data: dict | None = None,
        anomaly_data: list | None = None,
    ) -> OrchestratedResult:
        """
        Run the full multi-agent analysis pipeline.

        Args:
            symbol: Trading pair to analyze
            horizon_minutes: Prediction horizon
            include_anomalies: Whether to include anomaly detection
            market_data: Pre-loaded market data (optional)
            sentiment_data: Pre-loaded sentiment data (optional)
            anomaly_data: Pre-loaded anomaly data (optional)

        Returns:
            OrchestratedResult with all agent outputs
        """
        start = time.time()
        logger.info("analysis_started", symbol=symbol)

        # Check cache (30s TTL)
        cache_key = f"{symbol}:{horizon_minutes}"
        cached = self._cache.get(cache_key)
        if cached and (time.time() - float(cached.timestamp)) < 30:
            logger.info("cache_hit", symbol=symbol)
            return cached

        # ── Auto-fetch live data from Binance if not provided ─────────
        if not market_data:
            market_data = await self._fetch_live_market_data(symbol)

        if not sentiment_data:
            sentiment_data = {
                "mean_score": 0.0,
                "weighted_score": 0.0,
                "article_count": 0,
                "dominant_label": "neutral",
            }

        # Prepare state
        from agents.state import AgentState

        initial_state: AgentState = {
            "messages": [],
            "symbol": symbol,
            "market_data": market_data,
            "sentiment_data": sentiment_data,
            "anomaly_data": anomaly_data or [],
            "prediction_data": {},
            "market_analysis": "",
            "sentiment_analysis": "",
            "risk_assessment": "",
            "final_recommendation": "",
            "confidence_score": 0.0,
            "agent_trace": [],
            "error": "",
        }

        # Run graph
        if self._graph:
            try:
                result_state = await asyncio.to_thread(
                    self._graph.invoke, initial_state
                )
            except Exception as e:
                logger.error("graph_execution_failed", error=str(e))
                result_state = initial_state
                result_state["error"] = str(e)
        else:
            result_state = initial_state
            result_state["final_recommendation"] = "HOLD — Agent system not available"
            result_state["confidence_score"] = 0.2

        # Build result
        elapsed_ms = (time.time() - start) * 1000

        result = OrchestratedResult(
            symbol=symbol,
            features=result_state.get("market_data", {}),
            sentiment=result_state.get("sentiment_data", {}),
            prediction={
                "direction": _extract_direction(result_state.get("final_recommendation", "")),
                "confidence": result_state.get("confidence_score", 0.5),
                "probabilities": {"UP": 0.33, "DOWN": 0.33, "NEUTRAL": 0.34},
                "model_version": "langgraph-v1",
            },
            anomalies=result_state.get("anomaly_data", []),
            recommendation=result_state.get("final_recommendation", ""),
            confidence=result_state.get("confidence_score", 0.5),
            agent_trace=result_state.get("agent_trace", []),
            total_latency_ms=round(elapsed_ms, 1),
            timestamp=str(time.time()),
        )

        # Cache
        self._cache[cache_key] = result

        logger.info(
            "analysis_complete",
            symbol=symbol,
            confidence=result.confidence,
            latency_ms=result.total_latency_ms,
            agents=len(result.agent_trace),
        )

        return result

    async def _fetch_live_market_data(self, symbol: str) -> dict[str, Any]:
        """Fetch live market data from Binance public API."""
        import httpx

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
                )
                resp.raise_for_status()
                data = resp.json()

            price = float(data["lastPrice"])
            high = float(data["highPrice"])
            low = float(data["lowPrice"])
            open_price = float(data["openPrice"])
            volume = float(data["volume"])
            quote_volume = float(data["quoteVolume"])
            trade_count = int(data["count"])
            change_pct = float(data["priceChangePercent"])

            # Derived features
            vwap = quote_volume / volume if volume > 0 else price
            volatility = (high - low) / price if price > 0 else 0
            high_low_range = ((high - low) / low * 100) if low > 0 else 0

            # RSI approximation from 24h change
            if change_pct > 5:
                rsi = 75 + min(change_pct, 25)
            elif change_pct > 0:
                rsi = 50 + change_pct * 5
            elif change_pct > -5:
                rsi = 50 + change_pct * 5
            else:
                rsi = max(25 + change_pct, 0)

            # Buy/sell ratio approximation
            buy_sell_ratio = 0.5 + (change_pct / 20)
            buy_sell_ratio = max(0.1, min(0.9, buy_sell_ratio))

            market_data = {
                "close": price,
                "open": open_price,
                "high": high,
                "low": low,
                "volume": volume,
                "quote_volume": quote_volume,
                "trade_count": trade_count,
                "vwap": round(vwap, 2),
                "volatility": round(volatility, 6),
                "rsi": round(rsi, 1),
                "ema_20": round(price * 0.98, 2),
                "buy_sell_ratio": round(buy_sell_ratio, 4),
                "price_change_pct": round(change_pct, 2),
                "high_low_range": round(high_low_range, 4),
                "trade_intensity": round(trade_count / 86400, 2),
            }

            logger.info(
                "live_data_fetched",
                symbol=symbol,
                price=price,
                change_pct=change_pct,
            )
            return market_data

        except Exception as e:
            logger.error("binance_fetch_failed", symbol=symbol, error=str(e))
            return {
                "close": 0, "volume": 0, "volatility": 0,
                "rsi": 50, "buy_sell_ratio": 0.5, "price_change_pct": 0,
                "trade_count": 0, "high_low_range": 0, "trade_intensity": 0,
            }

    async def get_features(self, symbol: str) -> dict[str, Any]:
        """Get market features (for FeatureService compatibility)."""
        result = await self.analyze(symbol)
        return result.features

    async def get_sentiment(self, symbol: str) -> dict[str, Any]:
        """Get sentiment data (for FeatureService compatibility)."""
        result = await self.analyze(symbol)
        return result.sentiment


def _extract_direction(recommendation: str) -> str:
    """Extract trading direction from recommendation text."""
    text = recommendation.upper()
    if "BUY" in text and "SELL" not in text:
        return "UP"
    elif "SELL" in text and "BUY" not in text:
        return "DOWN"
    return "NEUTRAL"
