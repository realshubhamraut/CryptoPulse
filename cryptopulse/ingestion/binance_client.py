"""
CryptoPulse - Binance WebSocket Client

Manages persistent WebSocket connections to Binance for real-time trade data.
Supports multiple trading pairs with automatic reconnection and rate limiting.
"""

import asyncio
import json
from datetime import datetime
from typing import Any, AsyncGenerator, Callable

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from cryptopulse.config import settings
from cryptopulse.logging import get_logger
from cryptopulse.models import RawTradeEvent

logger = get_logger(__name__, component="binance_client")


class BinanceWebSocketClient:
    """
    Binance WebSocket client for real-time trade streams.
    
    Features:
    - Multi-pair subscription via combined streams
    - Automatic reconnection with exponential backoff
    - Event batching for efficient downstream processing
    - Heartbeat monitoring
    """
    
    BASE_URL = "wss://stream.binance.com:9443"
    MAX_STREAMS_PER_CONNECTION = 1024  # Binance limit
    PING_INTERVAL = 180  # 3 minutes
    PING_TIMEOUT = 10
    
    def __init__(
        self,
        trading_pairs: list[str] | None = None,
        on_trade: Callable[[RawTradeEvent], Any] | None = None,
        batch_size: int = 100,
        batch_timeout_ms: int = 500,
    ):
        """
        Initialize the Binance WebSocket client.
        
        Args:
            trading_pairs: List of trading pairs to subscribe (e.g., ["BTCUSDT"])
            on_trade: Callback function for each trade event
            batch_size: Number of events to batch before callback
            batch_timeout_ms: Maximum wait time for batch in milliseconds
        """
        self.trading_pairs = trading_pairs or settings.binance.trading_pair_list
        self.on_trade = on_trade
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        
        self._websocket: websockets.WebSocketClientProtocol | None = None
        self._running = False
        self._event_queue: asyncio.Queue[RawTradeEvent] = asyncio.Queue()
        self._stats = {
            "messages_received": 0,
            "trades_processed": 0,
            "reconnections": 0,
            "errors": 0,
            "last_message_at": None,
        }
    
    @property
    def stream_url(self) -> str:
        """Build the combined stream URL for all trading pairs."""
        streams = [f"{pair.lower()}@trade" for pair in self.trading_pairs]
        return f"{self.BASE_URL}/stream?streams={'/'.join(streams)}"
    
    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is currently connected."""
        return self._websocket is not None and self._websocket.open
    
    @property
    def stats(self) -> dict[str, Any]:
        """Get current statistics."""
        return {**self._stats, "is_connected": self.is_connected}
    
    async def connect(self) -> None:
        """Establish WebSocket connection with retry logic."""
        await self._connect_with_retry()
    
    @retry(
        retry=retry_if_exception_type((WebSocketException, ConnectionError, OSError)),
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=1, max=60),
    )
    async def _connect_with_retry(self) -> None:
        """Connect with exponential backoff retry."""
        logger.info(
            "connecting_to_binance",
            url=self.stream_url[:100] + "...",
            pairs_count=len(self.trading_pairs),
        )
        
        self._websocket = await websockets.connect(
            self.stream_url,
            ping_interval=self.PING_INTERVAL,
            ping_timeout=self.PING_TIMEOUT,
            close_timeout=10,
        )
        
        self._stats["reconnections"] += 1
        logger.info("connected_to_binance", reconnection_count=self._stats["reconnections"])
    
    async def disconnect(self) -> None:
        """Close the WebSocket connection."""
        self._running = False
        if self._websocket:
            await self._websocket.close()
            self._websocket = None
            logger.info("disconnected_from_binance")
    
    async def stream_trades(self) -> AsyncGenerator[RawTradeEvent, None]:
        """
        Stream trade events from Binance.
        
        Yields:
            RawTradeEvent for each trade received
        """
        self._running = True
        
        while self._running:
            try:
                if not self.is_connected:
                    await self.connect()
                
                async for message in self._websocket:
                    if not self._running:
                        break
                    
                    try:
                        data = json.loads(message)
                        trade = self._parse_trade(data)
                        if trade:
                            self._stats["trades_processed"] += 1
                            self._stats["last_message_at"] = datetime.utcnow().isoformat()
                            yield trade
                    except json.JSONDecodeError as e:
                        logger.warning("invalid_json", error=str(e))
                        self._stats["errors"] += 1
                    except Exception as e:
                        logger.error("trade_parse_error", error=str(e))
                        self._stats["errors"] += 1
                        
            except ConnectionClosed as e:
                logger.warning(
                    "connection_closed",
                    code=e.code,
                    reason=e.reason,
                )
                self._websocket = None
                if self._running:
                    await asyncio.sleep(1)  # Brief pause before reconnect
                    
            except Exception as e:
                logger.error("stream_error", error=str(e), error_type=type(e).__name__)
                self._stats["errors"] += 1
                self._websocket = None
                if self._running:
                    await asyncio.sleep(5)
    
    async def stream_batched_trades(
        self,
    ) -> AsyncGenerator[list[RawTradeEvent], None]:
        """
        Stream trade events in batches for efficient processing.
        
        Yields batches when either:
        - batch_size trades accumulated, OR
        - batch_timeout_ms elapsed since first trade in batch
        
        Yields:
            List of RawTradeEvent (up to batch_size)
        """
        batch: list[RawTradeEvent] = []
        batch_start_time: float | None = None
        timeout_seconds = self.batch_timeout_ms / 1000.0
        
        async for trade in self.stream_trades():
            if not batch:
                batch_start_time = asyncio.get_event_loop().time()
            
            batch.append(trade)
            
            # Check if batch is ready
            current_time = asyncio.get_event_loop().time()
            time_elapsed = current_time - batch_start_time if batch_start_time else 0
            
            if len(batch) >= self.batch_size or time_elapsed >= timeout_seconds:
                yield batch
                batch = []
                batch_start_time = None
        
        # Yield remaining trades
        if batch:
            yield batch
    
    def _parse_trade(self, data: dict[str, Any]) -> RawTradeEvent | None:
        """Parse raw WebSocket message into RawTradeEvent."""
        self._stats["messages_received"] += 1
        
        # Combined stream format: {"stream": "...", "data": {...}}
        if "data" in data:
            trade_data = data["data"]
        else:
            trade_data = data
        
        # Verify it's a trade event
        if trade_data.get("e") != "trade":
            return None
        
        try:
            return RawTradeEvent.model_validate(trade_data)
        except Exception as e:
            logger.warning("trade_validation_error", error=str(e), data=trade_data)
            return None


class BinanceOrderBookClient:
    """
    Binance WebSocket client for order book depth streams.
    
    Provides real-time order book updates for bid-ask spread
    and order imbalance calculations.
    """
    
    BASE_URL = "wss://stream.binance.com:9443"
    
    def __init__(
        self,
        trading_pairs: list[str] | None = None,
        depth_levels: int = 10,
    ):
        """
        Initialize the order book client.
        
        Args:
            trading_pairs: List of trading pairs
            depth_levels: Order book depth (5, 10, or 20)
        """
        self.trading_pairs = trading_pairs or settings.binance.trading_pair_list[:10]
        self.depth_levels = depth_levels
        
        # Local order book state
        self._order_books: dict[str, dict] = {}
        self._websocket = None
        self._running = False
    
    @property
    def stream_url(self) -> str:
        """Build the combined depth stream URL."""
        streams = [
            f"{pair.lower()}@depth{self.depth_levels}@100ms"
            for pair in self.trading_pairs
        ]
        return f"{self.BASE_URL}/stream?streams={'/'.join(streams)}"
    
    async def get_order_book(self, symbol: str) -> dict | None:
        """Get current order book state for a symbol."""
        return self._order_books.get(symbol.upper())
    
    async def stream_order_books(self) -> AsyncGenerator[dict, None]:
        """Stream order book updates."""
        self._running = True
        
        async with websockets.connect(
            self.stream_url,
            ping_interval=180,
            ping_timeout=10,
        ) as ws:
            self._websocket = ws
            
            async for message in ws:
                if not self._running:
                    break
                
                try:
                    data = json.loads(message)
                    if "data" in data:
                        book_data = data["data"]
                        symbol = data["stream"].split("@")[0].upper()
                        
                        order_book = {
                            "symbol": symbol,
                            "timestamp": datetime.utcnow(),
                            "bids": [
                                {"price": float(b[0]), "quantity": float(b[1])}
                                for b in book_data.get("bids", [])
                            ],
                            "asks": [
                                {"price": float(a[0]), "quantity": float(a[1])}
                                for a in book_data.get("asks", [])
                            ],
                        }
                        
                        # Calculate metrics
                        if order_book["bids"] and order_book["asks"]:
                            best_bid = order_book["bids"][0]["price"]
                            best_ask = order_book["asks"][0]["price"]
                            
                            bid_volume = sum(b["quantity"] for b in order_book["bids"])
                            ask_volume = sum(a["quantity"] for a in order_book["asks"])
                            
                            order_book["spread"] = best_ask - best_bid
                            order_book["spread_pct"] = (best_ask - best_bid) / best_bid * 100
                            order_book["imbalance"] = (
                                (bid_volume - ask_volume) / (bid_volume + ask_volume)
                                if (bid_volume + ask_volume) > 0 else 0
                            )
                        
                        self._order_books[symbol] = order_book
                        yield order_book
                        
                except Exception as e:
                    logger.warning("orderbook_parse_error", error=str(e))
    
    async def stop(self) -> None:
        """Stop the order book stream."""
        self._running = False
        if self._websocket:
            await self._websocket.close()


# =============================================================================
# Utility Functions
# =============================================================================

async def run_trade_ingestion(
    callback: Callable[[list[RawTradeEvent]], Any],
    trading_pairs: list[str] | None = None,
) -> None:
    """
    Run trade ingestion with batched callback.
    
    Args:
        callback: Async function to call with batched trades
        trading_pairs: Optional list of trading pairs to subscribe
    """
    client = BinanceWebSocketClient(trading_pairs=trading_pairs)
    
    try:
        async for batch in client.stream_batched_trades():
            await callback(batch)
    except asyncio.CancelledError:
        logger.info("trade_ingestion_cancelled")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    # Quick test
    async def main():
        client = BinanceWebSocketClient(trading_pairs=["BTCUSDT", "ETHUSDT"])
        
        count = 0
        async for trade in client.stream_trades():
            print(f"Trade: {trade.symbol} @ {trade.price} x {trade.quantity}")
            count += 1
            if count >= 10:
                break
        
        await client.disconnect()
        print(f"Stats: {client.stats}")
    
    asyncio.run(main())
