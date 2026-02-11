"""
CryptoPulse - Agent State Schema

Typed state for the LangGraph multi-agent system.
All agents read from and write to this shared state.
"""

from __future__ import annotations

from typing import Any, TypedDict


class AgentState(TypedDict, total=False):
    """
    Shared state passed through the LangGraph StateGraph.

    Fields:
        messages:              Chat messages for LLM context
        symbol:                Trading pair being analyzed
        market_data:           Latest market features from Gold layer
        sentiment_data:        Aggregated sentiment from Gold layer
        anomaly_data:          Detected anomalies from Gold layer
        prediction_data:       ML model prediction results

        market_analysis:       MarketAnalyst output
        sentiment_analysis:    SentimentAnalyst output
        risk_assessment:       RiskManager output
        final_recommendation:  PortfolioStrategist synthesis

        confidence_score:      Overall confidence (0-1)
        agent_trace:           Execution trace for debugging
        error:                 Error message if pipeline fails
    """

    # Input data
    messages: list[dict[str, str]]
    symbol: str
    market_data: dict[str, Any]
    sentiment_data: dict[str, Any]
    anomaly_data: list[dict[str, Any]]
    prediction_data: dict[str, Any]

    # Agent outputs
    market_analysis: str
    sentiment_analysis: str
    risk_assessment: str
    final_recommendation: str

    # Meta
    confidence_score: float
    agent_trace: list[str]
    error: str
