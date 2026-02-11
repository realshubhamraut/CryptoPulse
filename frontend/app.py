"""
CryptoPulse — Flask Frontend Dashboard

Professional presentation-ready dashboard for real-time crypto intelligence.
"""

import asyncio
import json
import os
import time
from datetime import datetime, timedelta

import httpx
from flask import Flask, render_template, jsonify, Response, request, stream_with_context
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Backend API URL
API_URL = os.getenv("CRYPTOPULSE_API_URL", "http://localhost:8000")

# Coin metadata
COINS = {
    "BTCUSDT": {"name": "Bitcoin", "short": "BTC", "icon": "₿"},
    "ETHUSDT": {"name": "Ethereum", "short": "ETH", "icon": "Ξ"},
    "BNBUSDT": {"name": "BNB", "short": "BNB", "icon": "◆"},
    "SOLUSDT": {"name": "Solana", "short": "SOL", "icon": "◎"},
    "XRPUSDT": {"name": "XRP", "short": "XRP", "icon": "✕"},
    "ADAUSDT": {"name": "Cardano", "short": "ADA", "icon": "♦"},
    "DOGEUSDT": {"name": "Dogecoin", "short": "DOGE", "icon": "Ð"},
    "AVAXUSDT": {"name": "Avalanche", "short": "AVAX", "icon": "▲"},
}


# =============================================================================
# Template Context
# =============================================================================

@app.context_processor
def inject_globals():
    return {
        "coins": COINS,
        "now": datetime.utcnow(),
        "api_url": API_URL,
    }


# =============================================================================
# Page Routes
# =============================================================================

@app.route("/")
def dashboard():
    return render_template("dashboard.html", page="dashboard")


@app.route("/news")
def news():
    return render_template("news.html", page="news")


@app.route("/analyze")
@app.route("/analyze/<symbol>")
def analyze(symbol=None):
    symbol = (symbol or "BTCUSDT").upper()
    return render_template("analyze.html", page="analyze", symbol=symbol)


@app.route("/ml")
def ml_pipeline():
    return render_template("ml.html", page="ml")


# =============================================================================
# API Proxy Routes
# =============================================================================

@app.route("/api/prices")
def api_prices():
    """Fetch live prices from Binance for all tracked coins."""
    try:
        symbols = list(COINS.keys())
        prices = {}

        with httpx.Client(timeout=10) as client:
            for symbol in symbols:
                try:
                    resp = client.get(
                        f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        prices[symbol] = {
                            "price": float(data["lastPrice"]),
                            "change": float(data["priceChangePercent"]),
                            "high": float(data["highPrice"]),
                            "low": float(data["lowPrice"]),
                            "volume": float(data["quoteVolume"]),
                            "trades": int(data["count"]),
                        }
                except Exception:
                    pass

        return jsonify(prices)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/analyze/<symbol>", methods=["POST"])
def api_analyze(symbol):
    """Proxy analysis request to FastAPI backend."""
    try:
        horizon = request.args.get("horizon", 5, type=int)
        with httpx.Client(timeout=120) as client:
            resp = client.post(
                f"{API_URL}/analyze/{symbol}?horizon_minutes={horizon}"
            )
            return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/health")
def api_health():
    """Proxy health check."""
    try:
        with httpx.Client(timeout=5) as client:
            resp = client.get(f"{API_URL}/health")
            return jsonify(resp.json())
    except Exception as e:
        return jsonify({"status": "unreachable", "error": str(e)}), 500


@app.route("/api/news")
def api_news():
    """Fetch crypto news from CryptoCompare public API."""
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=latest"
            )
            data = resp.json()

            articles = []
            for item in data.get("Data", [])[:30]:
                # Simple sentiment heuristic from title
                title = item.get("title", "").lower()
                body = item.get("body", "").lower()
                text = title + " " + body

                positive_words = ["surge", "rally", "bull", "gain", "rise", "soar", "high", "profit", "grow", "up", "breakout", "moon"]
                negative_words = ["crash", "dump", "bear", "drop", "fall", "plunge", "low", "loss", "fear", "down", "sell", "decline"]

                pos = sum(1 for w in positive_words if w in text)
                neg = sum(1 for w in negative_words if w in text)

                if pos > neg:
                    sentiment = "positive"
                elif neg > pos:
                    sentiment = "negative"
                else:
                    sentiment = "neutral"

                articles.append({
                    "id": item.get("id"),
                    "title": item.get("title", ""),
                    "body": item.get("body", "")[:200],
                    "source": item.get("source_info", {}).get("name", item.get("source", "Unknown")),
                    "url": item.get("url", "#"),
                    "image": item.get("imageurl", ""),
                    "published": item.get("published_on", 0),
                    "categories": item.get("categories", "").split("|")[:3],
                    "sentiment": sentiment,
                })

            return jsonify({"articles": articles, "count": len(articles)})
    except Exception as e:
        return jsonify({"error": str(e), "articles": []}), 500


@app.route("/api/news/stream")
def api_news_stream():
    """SSE endpoint for real-time news updates."""
    def generate():
        last_id = None
        while True:
            try:
                with httpx.Client(timeout=10) as client:
                    resp = client.get(
                        "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=latest"
                    )
                    data = resp.json()
                    articles = data.get("Data", [])[:5]

                    for article in articles:
                        article_id = article.get("id")
                        if article_id and article_id != last_id:
                            title = article.get("title", "").lower()
                            positive_words = ["surge", "rally", "bull", "gain", "rise", "soar"]
                            negative_words = ["crash", "dump", "bear", "drop", "fall", "plunge"]
                            pos = sum(1 for w in positive_words if w in title)
                            neg = sum(1 for w in negative_words if w in title)
                            sentiment = "positive" if pos > neg else ("negative" if neg > pos else "neutral")

                            payload = json.dumps({
                                "id": article_id,
                                "title": article.get("title", ""),
                                "source": article.get("source_info", {}).get("name", "Unknown"),
                                "url": article.get("url", "#"),
                                "sentiment": sentiment,
                                "published": article.get("published_on", 0),
                            })
                            yield f"data: {payload}\n\n"
                            last_id = article_id
                            break
            except Exception:
                pass

            time.sleep(30)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
