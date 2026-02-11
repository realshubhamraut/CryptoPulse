"""
CryptoPulse - LangGraph Agent Graph

Defines the multi-agent StateGraph that routes data through
specialist agents for comprehensive market intelligence.

Graph topology:
    START → MarketAnalyst  ─┐
          → SentimentAnalyst ├→ PortfolioStrategist → END
          → RiskManager     ─┘
"""

from __future__ import annotations

from typing import Any

from langgraph.graph import END, StateGraph

from agents.state import AgentState
from agents.market_analyst import market_analyst_node
from agents.sentiment_analyst import sentiment_analyst_node
from agents.risk_manager import risk_manager_node
from agents.portfolio_strategist import portfolio_strategist_node


def create_analysis_graph() -> Any:
    """
    Build and compile the CryptoPulse analysis StateGraph.

    The graph runs 3 specialist agents in parallel, then fuses
    their outputs through the PortfolioStrategist.

    Returns:
        Compiled LangGraph runnable
    """
    graph = StateGraph(AgentState)

    # ── Add nodes ──────────────────────────────────────────────
    graph.add_node("market_analyst", market_analyst_node)
    graph.add_node("sentiment_analyst", sentiment_analyst_node)
    graph.add_node("risk_manager", risk_manager_node)
    graph.add_node("portfolio_strategist", portfolio_strategist_node)

    # ── Fan-out: START → 3 parallel analysts ───────────────────
    graph.set_entry_point("market_analyst")

    # After market_analyst → sentiment_analyst → risk_manager → strategist
    # (LangGraph executes sequentially; for true parallelism,
    #  use asyncio in the orchestrator wrapper)
    graph.add_edge("market_analyst", "sentiment_analyst")
    graph.add_edge("sentiment_analyst", "risk_manager")
    graph.add_edge("risk_manager", "portfolio_strategist")

    # ── End ────────────────────────────────────────────────────
    graph.add_edge("portfolio_strategist", END)

    return graph.compile()


def create_analysis_graph_with_routing() -> Any:
    """
    Advanced graph with conditional routing.

    If anomaly data is present, routes through RiskManager first
    (elevated priority). Otherwise skips straight to analysis.
    """
    graph = StateGraph(AgentState)

    graph.add_node("market_analyst", market_analyst_node)
    graph.add_node("sentiment_analyst", sentiment_analyst_node)
    graph.add_node("risk_manager", risk_manager_node)
    graph.add_node("portfolio_strategist", portfolio_strategist_node)

    # Entry: always start with market analyst
    graph.set_entry_point("market_analyst")

    # Conditional: check if anomalies require urgent risk assessment
    def route_after_market(state: AgentState) -> str:
        anomalies = state.get("anomaly_data", [])
        if anomalies and len(anomalies) > 0:
            return "risk_manager"  # Priority path
        return "sentiment_analyst"  # Normal path

    graph.add_conditional_edges(
        "market_analyst",
        route_after_market,
        {
            "risk_manager": "risk_manager",
            "sentiment_analyst": "sentiment_analyst",
        },
    )

    # After risk_manager or sentiment_analyst → converge
    graph.add_edge("risk_manager", "sentiment_analyst")
    graph.add_edge("sentiment_analyst", "portfolio_strategist")
    graph.add_edge("portfolio_strategist", END)

    return graph.compile()
