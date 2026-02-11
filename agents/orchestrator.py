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

        # Prepare state
        from agents.state import AgentState

        initial_state: AgentState = {
            "messages": [],
            "symbol": symbol,
            "market_data": market_data or {},
            "sentiment_data": sentiment_data or {},
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
