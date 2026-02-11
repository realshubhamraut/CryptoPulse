"""
CryptoPulse - Sentiment Analyst Agent

NLP sentiment reasoning agent that interprets news sentiment data,
identifies narrative themes, and assesses market sentiment impact.
"""

from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from agents.state import AgentState


SENTIMENT_ANALYST_PROMPT = """You are the SentimentAnalyst agent for CryptoPulse, an expert in cryptocurrency news and sentiment analysis.

You will receive aggregated sentiment data including:
- Overall sentiment score (positive/negative)
- Confidence-weighted sentiment
- Article counts and sentiment distribution
- Dominant sentiment label

Your job is to:
1. Interpret the overall market sentiment direction
2. Assess the strength and reliability of the sentiment signal (based on volume and confidence)
3. Identify if sentiment is diverging from price action (contrarian signals)
4. Evaluate news volume — is it unusually high or low?
5. Provide a sentiment-based outlook with clear reasoning

Be concise and data-driven. Flag any low-confidence signals."""


def sentiment_analyst_node(state: AgentState) -> dict[str, Any]:
    """
    SentimentAnalyst agent node for the LangGraph StateGraph.

    Analyzes sentiment data and produces a sentiment assessment.
    """
    from langchain_groq import ChatGroq

    symbol = state.get("symbol", "UNKNOWN")
    sentiment_data = state.get("sentiment_data", {})
    market_data = state.get("market_data", {})

    sentiment_context = json.dumps(
        {k: str(v) for k, v in sentiment_data.items()},
        indent=2,
    )

    # Include price context for divergence detection
    price_change = market_data.get("price_change_pct", 0)

    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0.1,
    )

    messages = [
        SystemMessage(content=SENTIMENT_ANALYST_PROMPT),
        HumanMessage(content=f"""Analyze the following sentiment data for {symbol}:

```json
{sentiment_context}
```

Current price change: {price_change}%

Provide your sentiment analysis:"""),
    ]

    try:
        response = llm.invoke(messages)
        analysis = response.content

        trace = state.get("agent_trace", [])
        trace.append(f"SentimentAnalyst: analyzed {symbol} sentiment ({len(sentiment_data)} fields)")

        return {
            "sentiment_analysis": analysis,
            "agent_trace": trace,
        }

    except Exception as e:
        trace = state.get("agent_trace", [])
        trace.append(f"SentimentAnalyst: ERROR - {str(e)}")

        score = float(sentiment_data.get("weighted_score", sentiment_data.get("mean_score", 0)))
        article_count = int(sentiment_data.get("article_count", 0))
        dominant = sentiment_data.get("dominant_label", "neutral")

        if score > 0.3:
            outlook = "BULLISH sentiment — news narrative is positive"
        elif score < -0.3:
            outlook = "BEARISH sentiment — negative news pressure"
        else:
            outlook = "NEUTRAL sentiment — mixed signals"

        fallback = (
            f"**Sentiment Analysis for {symbol}** (heuristic fallback)\n\n"
            f"- Outlook: {outlook}\n"
            f"- Score: {score:.3f}\n"
            f"- Articles: {article_count}\n"
            f"- Dominant Label: {dominant}\n"
        )

        return {
            "sentiment_analysis": fallback,
            "agent_trace": trace,
        }
