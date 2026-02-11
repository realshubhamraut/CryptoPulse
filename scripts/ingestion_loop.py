"""
CryptoPulse - Ingestion Loop

Continuous timer loop for Docker container that runs Binance trade
and news ingestion on a configurable interval.

Usage (from Docker):
    python -m scripts.ingestion_loop

Usage (local):
    python scripts/ingestion_loop.py
"""

from __future__ import annotations

import asyncio
import signal
import sys
import time


TRADE_INTERVAL = 30   # seconds
NEWS_INTERVAL = 60    # seconds

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    print(f"\n⚡ Received signal {signum}, shutting down gracefully...")
    _shutdown = True


async def main():
    """Main ingestion loop."""
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    print("═" * 50)
    print("  CryptoPulse Ingestion Loop")
    print(f"  Trade interval: {TRADE_INTERVAL}s")
    print(f"  News interval:  {NEWS_INTERVAL}s")
    print("═" * 50)

    cycle = 0

    while not _shutdown:
        cycle += 1
        print(f"\n─── Cycle {cycle} ───")

        # Trade ingestion (every cycle)
        try:
            from functions.binance_ingestion import ingest_trades
            result = await ingest_trades(publish=True)
            print(f"  ✓ Trades: {result.get('total_published', 0)} published")
        except Exception as e:
            print(f"  ✗ Trades error: {e}")

        # News ingestion (every other cycle ≈ 60s)
        if cycle % 2 == 0:
            try:
                from functions.news_ingestion import ingest_news
                result = await ingest_news(publish=True)
                print(f"  ✓ News: {result.get('total_published', 0)} published")
            except Exception as e:
                print(f"  ✗ News error: {e}")

        # Sleep with shutdown check
        for _ in range(TRADE_INTERVAL):
            if _shutdown:
                break
            await asyncio.sleep(1)

    print("\n✓ Ingestion loop stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
