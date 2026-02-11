"""
CryptoPulse - Portfolio Strategist Agent

Final synthesis agent that fuses signals from all specialist agents
into actionable trading recommendations with confidence scores.
"""

from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from agents.state import AgentState


PORTFOLIO_STRATEGIST_PROMPT = """You are the PortfolioStrategist agent for CryptoPulse, the final decision-making intelligence.

You receive analysis from three specialist agents:
1. **MarketAnalyst** — Technical analysis, trend, support/resistance
2. **SentimentAnalyst** — News sentiment, narrative themes
3. **RiskManager** — Risk assessment, anomaly evaluation

Your job is to synthesize all signals into:

1. **ACTION**: BUY / SELL / HOLD with specific reasoning
2. **CONFIDENCE**: 0-100% confidence in the recommendation
3. **TIMEFRAME**: Short-term (1-5min), Medium (15-60min), or Long (1-24h)
4. **POSITION SIZE**: Full / Reduced / Minimal based on risk
5. **KEY FACTORS**: Top 3 factors driving the decision
6. **STOP LOSS**: Suggested stop-loss level
7. **TAKE PROFIT**: Suggested take-profit level

Rules:
- If analyst signals conflict, weight technical analysis 40%, sentiment 30%, risk 30%
- If risk is HIGH or EXTREME, always recommend HOLD or reduced position
- Never recommend against the risk manager's assessment
- Be specific with entry/exit levels when possible
- Provide a clear, actionable summary

Format your response clearly with headers for each section."""


def portfolio_strategist_node(state: AgentState) -> dict[str, Any]:
    """
    PortfolioStrategist agent node for the LangGraph StateGraph.

    Synthesizes all agent analyses into a final recommendation.
    """
    from langchain_groq import ChatGroq

    symbol = state.get("symbol", "UNKNOWN")
    market_analysis = state.get("market_analysis", "No market analysis available.")
    sentiment_analysis = state.get("sentiment_analysis", "No sentiment analysis available.")
    risk_assessment = state.get("risk_assessment", "No risk assessment available.")
    market_data = state.get("market_data", {})

    current_price = market_data.get("close", "N/A")

    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0.2,
    )

    messages = [
        SystemMessage(content=PORTFOLIO_STRATEGIST_PROMPT),
        HumanMessage(content=f"""Synthesize the following analyses for {symbol} (current price: {current_price}):

---
## Market Analyst Report
{market_analysis}

---
## Sentiment Analyst Report
{sentiment_analysis}

---
## Risk Manager Report
{risk_assessment}

---

Provide your final recommendation:"""),
    ]

    try:
        response = llm.invoke(messages)
        recommendation = response.content

        # Extract confidence from the response (heuristic)
        confidence = _extract_confidence(recommendation)

        trace = state.get("agent_trace", [])
        trace.append(f"PortfolioStrategist: synthesized recommendation (confidence: {confidence:.0%})")

        return {
            "final_recommendation": recommendation,
            "confidence_score": confidence,
            "agent_trace": trace,
        }

    except Exception as e:
        trace = state.get("agent_trace", [])
        trace.append(f"PortfolioStrategist: ERROR - {str(e)}")

        fallback = (
            f"**Recommendation for {symbol}** (heuristic fallback)\n\n"
            f"- Action: HOLD\n"
            f"- Confidence: 30%\n"
            f"- Reason: Unable to run full LLM synthesis. Defaulting to HOLD.\n"
        )

        return {
            "final_recommendation": fallback,
            "confidence_score": 0.3,
            "agent_trace": trace,
        }


def _extract_confidence(text: str) -> float:
    """Extract confidence score from LLM response text."""
    import re

    # Look for patterns like "Confidence: 75%" or "85% confidence"
    patterns = [
        r"[Cc]onfidence[:\s]+(\d{1,3})%",
        r"(\d{1,3})%\s*[Cc]onfidence",
        r"[Cc]onfidence[:\s]+0\.(\d{1,2})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            val = float(match.group(1))
            return val / 100 if val > 1 else val

    # Heuristic: estimate from recommendation text
    text_lower = text.lower()
    if any(w in text_lower for w in ["strong buy", "highly confident", "very bullish"]):
        return 0.85
    elif any(w in text_lower for w in ["buy", "bullish", "confident"]):
        return 0.7
    elif any(w in text_lower for w in ["hold", "neutral", "wait"]):
        return 0.5
    elif any(w in text_lower for w in ["sell", "bearish"]):
        return 0.6
    return 0.5
