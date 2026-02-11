"""
CryptoPulse - Binance Trade Ingestion Function

Timer-triggered Azure Function that fetches recent trades from
Binance REST API and publishes them to Kafka / Event Hubs.

Design decisions:
  - Uses REST API (not WebSocket) for serverless compatibility
  - Validates each trade through Pydantic RawTradeEvent model
  - Batches events for efficient Kafka publishing
  - Idempotent via trade_id deduplication
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from cryptopulse.config import settings
from cryptopulse.logging import get_logger
from cryptopulse.models import RawTradeEvent

logger = get_logger(__name__, component="binance_ingestion")


# =============================================================================
# Binance REST Client
# =============================================================================

BINANCE_BASE_URL = "https://api.binance.com/api/v3"
DEFAULT_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]


async def fetch_recent_trades(
    symbol: str,
    limit: int = 100,
    client: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch recent trades from Binance REST API.

    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        limit: Number of trades to fetch (max 1000)
        client: Optional httpx client for connection reuse

    Returns:
        List of raw trade dicts
    """
    url = f"{BINANCE_BASE_URL}/trades"
    params = {"symbol": symbol, "limit": min(limit, 1000)}

    should_close = client is None
    client = client or httpx.AsyncClient(timeout=10.0)

    try:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(
            "binance_api_error",
            symbol=symbol,
            status=e.response.status_code,
            body=e.response.text[:200],
        )
        return []
    except httpx.RequestError as e:
        logger.error("binance_request_error", symbol=symbol, error=str(e))
        return []
    finally:
        if should_close:
            await client.aclose()


# =============================================================================
# Trade Validation & Transform
# =============================================================================

def validate_and_transform(raw_trade: dict, symbol: str) -> dict[str, Any] | None:
    """
    Validate a raw Binance trade and transform to our event schema.

    Returns None if validation fails.
    """
    try:
        # Binance REST trades have a different format than WebSocket
        # REST: {id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch}
        event = {
            "e": "trade",
            "E": raw_trade["time"],
            "s": symbol,
            "t": raw_trade["id"],
            "p": str(raw_trade["price"]),
            "q": str(raw_trade["qty"]),
            "b": 0,  # REST doesn't expose order IDs
            "a": 0,
            "T": raw_trade["time"],
            "m": raw_trade["isBuyerMaker"],
            "M": raw_trade.get("isBestMatch", True),
        }

        # Validate through Pydantic model
        validated = RawTradeEvent.model_validate(event)
        return validated.model_dump(by_alias=True)

    except Exception as e:
        logger.warning(
            "trade_validation_failed",
            trade_id=raw_trade.get("id"),
            error=str(e),
        )
        return None


# =============================================================================
# Deduplication
# =============================================================================

class TradeDeduplicator:
    """
    In-memory deduplication using a bounded set of recent trade IDs.

    For serverless, this resets per invocation. For persistent dedup,
    use Redis or Event Hubs' built-in dedup.
    """

    def __init__(self, max_size: int = 10_000):
        self._seen: set[int] = set()
        self._max_size = max_size

    def is_new(self, trade_id: int) -> bool:
        """Check if trade_id is new (not yet seen)."""
        if trade_id in self._seen:
            return False
        self._seen.add(trade_id)
        if len(self._seen) > self._max_size:
            # Evict oldest entries (rough LRU)
            to_remove = sorted(self._seen)[: self._max_size // 2]
            self._seen -= set(to_remove)
        return True


# =============================================================================
# Main Ingestion Function
# =============================================================================

async def ingest_trades(
    trading_pairs: list[str] | None = None,
    trades_per_pair: int = 100,
    publish: bool = True,
) -> dict[str, Any]:
    """
    Main ingestion entry point.

    Fetches recent trades for all configured pairs, validates,
    deduplicates, and publishes to Kafka/Event Hubs.

    Args:
        trading_pairs: Override trading pairs (default: from settings)
        trades_per_pair: Number of trades to fetch per pair
        publish: Whether to actually publish (False for testing)

    Returns:
        Summary dict with counts
    """
    pairs = trading_pairs or DEFAULT_PAIRS
    dedup = TradeDeduplicator()

    start_time = time.monotonic()
    total_fetched = 0
    total_valid = 0
    total_published = 0

    logger.info(
        "starting_trade_ingestion",
        pairs=pairs,
        trades_per_pair=trades_per_pair,
    )

    # Publisher (lazy import to avoid import errors when kafka isn't available)
    publisher = None
    if publish:
        try:
            from cryptopulse.ingestion.kafka_producer import TradeEventPublisher

            publisher = TradeEventPublisher()
        except Exception as e:
            logger.warning("publisher_unavailable", error=str(e))

    async with httpx.AsyncClient(timeout=10.0) as client:
        for symbol in pairs:
            raw_trades = await fetch_recent_trades(
                symbol, limit=trades_per_pair, client=client
            )
            total_fetched += len(raw_trades)

            # Validate and deduplicate
            valid_events = []
            for raw in raw_trades:
                trade_id = raw.get("id", 0)
                if not dedup.is_new(trade_id):
                    continue

                event = validate_and_transform(raw, symbol)
                if event:
                    valid_events.append(event)

            total_valid += len(valid_events)

            # Publish batch
            if publisher and valid_events:
                try:
                    messages = [(symbol, ev) for ev in valid_events]
                    publisher.publish_batch(
                        topic=settings.kafka.trades_topic,
                        messages=messages,
                    )
                    total_published += len(valid_events)
                except Exception as e:
                    logger.error(
                        "publish_error",
                        symbol=symbol,
                        error=str(e),
                    )

            logger.info(
                "pair_ingested",
                symbol=symbol,
                fetched=len(raw_trades),
                valid=len(valid_events),
            )

    elapsed = time.monotonic() - start_time

    summary = {
        "pairs_processed": len(pairs),
        "total_fetched": total_fetched,
        "total_valid": total_valid,
        "total_published": total_published,
        "elapsed_seconds": round(elapsed, 3),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    logger.info("trade_ingestion_complete", **summary)

    return summary


# =============================================================================
# Azure Function / CLI Entry Point
# =============================================================================

def main() -> None:
    """
    Entry point for Azure Functions timer trigger or CLI.

    When deployed as an Azure Function, this is invoked by the
    function host. For local testing, run directly.
    """
    result = asyncio.run(ingest_trades())
    print(f"Ingestion complete: {result}")


if __name__ == "__main__":
    main()
