"""
CryptoPulse - Market Analyst Agent

Deep market analysis agent that interprets technical indicators,
identifies trends, and provides market structure insights using LLM reasoning.
"""

from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from agents.state import AgentState


MARKET_ANALYST_PROMPT = """You are the MarketAnalyst agent for CryptoPulse, an expert in cryptocurrency technical analysis.

You will receive real-time market data including OHLCV, VWAP, volatility, RSI, EMA, Bollinger Bands, buy/sell ratios, and trade intensity.

Your job is to:
1. Identify the current market trend (bullish, bearish, or neutral)
2. Analyze key support and resistance levels based on price action
3. Interpret technical indicators (RSI overbought/oversold, EMA crossovers, Bollinger Band squeeze)
4. Assess volume profile and trade intensity for confirmation
5. Identify any divergences between price and indicators

Provide your analysis in a structured format with clear reasoning. Be specific with numbers.
Never hallucinate data — only use what is provided."""


def market_analyst_node(state: AgentState) -> dict[str, Any]:
    """
    MarketAnalyst agent node for the LangGraph StateGraph.

    Analyzes market data and produces a technical analysis summary.
    """
    from langchain_groq import ChatGroq

    symbol = state.get("symbol", "UNKNOWN")
    market_data = state.get("market_data", {})

    # Format market data for LLM
    data_context = json.dumps(
        {k: str(v) for k, v in market_data.items()},
        indent=2,
    )

    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0.1,
    )

    messages = [
        SystemMessage(content=MARKET_ANALYST_PROMPT),
        HumanMessage(content=f"""Analyze the following market data for {symbol}:

```json
{data_context}
```

Provide your technical analysis:"""),
    ]

    try:
        response = llm.invoke(messages)
        analysis = response.content

        trace = state.get("agent_trace", [])
        trace.append(f"MarketAnalyst: analyzed {symbol} ({len(market_data)} features)")

        return {
            "market_analysis": analysis,
            "agent_trace": trace,
        }

    except Exception as e:
        trace = state.get("agent_trace", [])
        trace.append(f"MarketAnalyst: ERROR - {str(e)}")

        # Fallback heuristic analysis
        rsi = float(market_data.get("rsi", 50))
        volatility = float(market_data.get("volatility", 0))
        buy_sell = float(market_data.get("buy_sell_ratio", 0.5))
        price_change = float(market_data.get("price_change_pct", 0))

        if rsi > 70:
            trend = "OVERBOUGHT — potential reversal"
        elif rsi < 30:
            trend = "OVERSOLD — potential bounce"
        elif price_change > 0.5:
            trend = "BULLISH — upward momentum"
        elif price_change < -0.5:
            trend = "BEARISH — downward pressure"
        else:
            trend = "NEUTRAL — range-bound"

        fallback = (
            f"**Market Analysis for {symbol}** (heuristic fallback)\n\n"
            f"- Trend: {trend}\n"
            f"- RSI: {rsi:.1f}\n"
            f"- Volatility: {volatility:.4f}\n"
            f"- Buy/Sell Ratio: {buy_sell:.2f}\n"
            f"- Price Change: {price_change:.2f}%\n"
        )

        return {
            "market_analysis": fallback,
            "agent_trace": trace,
        }
