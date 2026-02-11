"""
CryptoPulse - Kafka Producer

Publishes events to Kafka topics (or Azure Event Hubs).
Supports both local Kafka and Azure Event Hubs backends.
"""

import asyncio
import json
from datetime import datetime
from typing import Any

from confluent_kafka import Producer, KafkaError, KafkaException
from pydantic import BaseModel

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="kafka_producer")


class EventPublisher:
    """
    Event publisher for Kafka/Event Hubs.
    
    Handles serialization, batching, and delivery confirmation.
    """
    
    def __init__(
        self,
        bootstrap_servers: str | None = None,
        batch_size: int = 100,
        linger_ms: int = 5,
    ):
        """
        Initialize the event publisher.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            batch_size: Number of messages to batch
            linger_ms: Time to wait for batching (milliseconds)
        """
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        
        self._producer: Producer | None = None
        self._delivery_callbacks: dict[str, asyncio.Future] = {}
        
        self._config = {
            "bootstrap.servers": self.bootstrap_servers,
            "batch.size": batch_size * 1024,  # bytes
            "linger.ms": linger_ms,
            "compression.type": "lz4",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
            "enable.idempotence": True,
        }
        
        self._stats = {
            "messages_sent": 0,
            "messages_failed": 0,
            "bytes_sent": 0,
        }
    
    def connect(self) -> None:
        """Initialize the Kafka producer."""
        logger.info("connecting_kafka_producer", servers=self.bootstrap_servers)
        self._producer = Producer(self._config)
        logger.info("kafka_producer_connected")
    
    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        """Handle delivery confirmation."""
        if err:
            logger.error(
                "message_delivery_failed",
                error=str(err),
                topic=msg.topic() if msg else None,
            )
            self._stats["messages_failed"] += 1
        else:
            self._stats["messages_sent"] += 1
            self._stats["bytes_sent"] += len(msg.value()) if msg.value() else 0
    
    def publish(
        self,
        topic: str,
        key: str | None,
        value: dict | BaseModel,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Publish a single message to a topic.
        
        Args:
            topic: Kafka topic name
            key: Message key (for partitioning)
            value: Message value (dict or Pydantic model)
            headers: Optional message headers
        """
        if not self._producer:
            raise RuntimeError("Producer not connected. Call connect() first.")
        
        # Serialize value
        if isinstance(value, BaseModel):
            value_bytes = value.model_dump_json().encode("utf-8")
        else:
            value_bytes = json.dumps(value, default=str).encode("utf-8")
        
        key_bytes = key.encode("utf-8") if key else None
        
        # Convert headers
        kafka_headers = None
        if headers:
            kafka_headers = [(k, v.encode("utf-8")) for k, v in headers.items()]
        
        try:
            self._producer.produce(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                headers=kafka_headers,
                callback=self._delivery_callback,
            )
        except KafkaException as e:
            logger.error("produce_error", error=str(e), topic=topic)
            raise
    
    def publish_batch(
        self,
        topic: str,
        messages: list[tuple[str | None, dict | BaseModel]],
    ) -> None:
        """
        Publish a batch of messages.
        
        Args:
            topic: Kafka topic name
            messages: List of (key, value) tuples
        """
        for key, value in messages:
            self.publish(topic, key, value)
        
        # Trigger delivery
        self._producer.poll(0)
    
    def flush(self, timeout: float = 10.0) -> int:
        """
        Flush all pending messages.
        
        Args:
            timeout: Maximum wait time in seconds
            
        Returns:
            Number of messages still in queue
        """
        if self._producer:
            return self._producer.flush(timeout)
        return 0
    
    def close(self) -> None:
        """Close the producer, flushing pending messages."""
        if self._producer:
            remaining = self.flush(timeout=30.0)
            if remaining > 0:
                logger.warning("unflushed_messages", count=remaining)
            self._producer = None
            logger.info("kafka_producer_closed", stats=self._stats)
    
    @property
    def stats(self) -> dict[str, Any]:
        """Get producer statistics."""
        return self._stats.copy()
    
    def __enter__(self) -> "EventPublisher":
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class TradeEventPublisher:
    """Specialized publisher for trade events."""
    
    def __init__(self, publisher: EventPublisher | None = None):
        self.publisher = publisher or EventPublisher()
        self.topic = settings.kafka.trades_topic
    
    def publish_trades(self, trades: list) -> None:
        """Publish a batch of trade events."""
        from cryptopulse.models import RawTradeEvent
        
        messages = [
            (trade.symbol, trade if isinstance(trade, RawTradeEvent) else trade)
            for trade in trades
        ]
        
        self.publisher.publish_batch(self.topic, messages)
        logger.debug("trades_published", count=len(trades))


class NewsEventPublisher:
    """Specialized publisher for news events."""
    
    def __init__(self, publisher: EventPublisher | None = None):
        self.publisher = publisher or EventPublisher()
        self.topic = settings.kafka.news_topic
    
    def publish_news(self, articles: list) -> None:
        """Publish a batch of news articles."""
        messages = [
            (article.get("article_id") or article.get("source"), article)
            for article in articles
        ]
        
        self.publisher.publish_batch(self.topic, messages)
        logger.debug("news_published", count=len(articles))


# =============================================================================
# Azure Event Hubs Publisher (for Azure deployment)
# =============================================================================

class AzureEventHubPublisher:
    """
    Azure Event Hubs publisher for cloud deployment.
    
    Uses the Azure Event Hubs SDK instead of Kafka.
    """
    
    def __init__(
        self,
        connection_string: str | None = None,
        eventhub_name: str | None = None,
    ):
        self.connection_string = (
            connection_string or settings.azure_eventhub.connection_string
        )
        self.eventhub_name = eventhub_name
        self._producer = None
        self._stats = {"messages_sent": 0, "messages_failed": 0}
    
    async def connect(self) -> None:
        """Initialize the Event Hub producer."""
        from azure.eventhub.aio import EventHubProducerClient
        
        self._producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string,
            eventhub_name=self.eventhub_name,
        )
        logger.info("eventhub_producer_connected", hub=self.eventhub_name)
    
    async def publish_batch(self, events: list[dict | BaseModel]) -> None:
        """Publish a batch of events to Event Hub."""
        from azure.eventhub import EventData
        
        if not self._producer:
            raise RuntimeError("Producer not connected")
        
        async with self._producer:
            batch = await self._producer.create_batch()
            
            for event in events:
                if isinstance(event, BaseModel):
                    data = event.model_dump_json()
                else:
                    data = json.dumps(event, default=str)
                
                try:
                    batch.add(EventData(data))
                except ValueError:
                    # Batch is full, send it and create new one
                    await self._producer.send_batch(batch)
                    self._stats["messages_sent"] += batch.size_in_bytes
                    batch = await self._producer.create_batch()
                    batch.add(EventData(data))
            
            if batch.size_in_bytes > 0:
                await self._producer.send_batch(batch)
                self._stats["messages_sent"] += len(events)
    
    async def close(self) -> None:
        """Close the producer."""
        if self._producer:
            await self._producer.close()
            logger.info("eventhub_producer_closed", stats=self._stats)
