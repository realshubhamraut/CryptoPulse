"""
CryptoPulse - Multi-Agent Analytics Package

LangGraph-powered multi-agent system with 4 specialist agents:
    - MarketAnalyst:       Technical indicator analysis
    - SentimentAnalyst:    NLP sentiment reasoning
    - RiskManager:         Anomaly assessment & risk scoring
    - PortfolioStrategist: Signal fusion & trading recommendations

Usage:
    from agents.graph import create_analysis_graph
    from agents.orchestrator import AgentOrchestrator
"""

from agents.state import AgentState
from agents.orchestrator import AgentOrchestrator, OrchestratedResult

__all__ = [
    "AgentState",
    "AgentOrchestrator",
    "OrchestratedResult",
]
