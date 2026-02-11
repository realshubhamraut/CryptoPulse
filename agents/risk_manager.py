"""
CryptoPulse - Risk Manager Agent

Risk assessment agent that evaluates anomaly signals,
computes risk scores, and provides risk-adjusted recommendations.
"""

from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from agents.state import AgentState


RISK_MANAGER_PROMPT = """You are the RiskManager agent for CryptoPulse, an expert in cryptocurrency risk assessment and market manipulation detection.

You will receive:
- Market data (volatility, volume, price action)
- Anomaly detection results (if any unusual patterns were detected)
- Anomaly types: pump, dump, wash_trade, volume_spike, price_manipulation

Your job is to:
1. Assess the overall risk level (LOW / MODERATE / HIGH / EXTREME)
2. Evaluate any detected anomalies and their severity
3. Compute a risk-adjusted confidence score
4. Recommend position sizing adjustments based on risk
5. Identify specific risk factors (volatility, low liquidity, manipulation signals)
6. Suggest stop-loss levels if applicable

Be conservative — when in doubt, flag higher risk. Protect capital first."""


def risk_manager_node(state: AgentState) -> dict[str, Any]:
    """
    RiskManager agent node for the LangGraph StateGraph.

    Evaluates risk from market data and anomaly signals.
    """
    from langchain_google_genai import ChatGoogleGenerativeAI

    symbol = state.get("symbol", "UNKNOWN")
    market_data = state.get("market_data", {})
    anomaly_data = state.get("anomaly_data", [])

    # Format anomaly data
    anomaly_context = json.dumps(anomaly_data, indent=2, default=str) if anomaly_data else "No anomalies detected."

    market_context = json.dumps({
        "volatility": str(market_data.get("volatility", 0)),
        "volume": str(market_data.get("volume", 0)),
        "trade_count": str(market_data.get("trade_count", 0)),
        "buy_sell_ratio": str(market_data.get("buy_sell_ratio", 0.5)),
        "price_change_pct": str(market_data.get("price_change_pct", 0)),
        "high_low_range": str(market_data.get("high_low_range", 0)),
    }, indent=2)

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash",
        temperature=0.1,
    )

    messages = [
        SystemMessage(content=RISK_MANAGER_PROMPT),
        HumanMessage(content=f"""Assess risk for {symbol}:

**Market Context:**
```json
{market_context}
```

**Anomaly Detection Results:**
```json
{anomaly_context}
```

Provide your risk assessment:"""),
    ]

    try:
        response = llm.invoke(messages)
        assessment = response.content

        trace = state.get("agent_trace", [])
        trace.append(f"RiskManager: assessed {symbol} risk ({len(anomaly_data)} anomalies)")

        return {
            "risk_assessment": assessment,
            "agent_trace": trace,
        }

    except Exception as e:
        trace = state.get("agent_trace", [])
        trace.append(f"RiskManager: ERROR - {str(e)}")

        volatility = float(market_data.get("volatility", 0))
        has_anomalies = len(anomaly_data) > 0

        if has_anomalies:
            risk_level = "HIGH"
        elif volatility > 0.05:
            risk_level = "MODERATE"
        else:
            risk_level = "LOW"

        fallback = (
            f"**Risk Assessment for {symbol}** (heuristic fallback)\n\n"
            f"- Risk Level: {risk_level}\n"
            f"- Anomalies Detected: {len(anomaly_data)}\n"
            f"- Volatility: {volatility:.4f}\n"
            f"- Recommendation: {'Reduce position size' if risk_level == 'HIGH' else 'Standard position'}\n"
        )

        return {
            "risk_assessment": fallback,
            "agent_trace": trace,
        }
