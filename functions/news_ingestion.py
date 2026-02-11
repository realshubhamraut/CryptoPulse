"""
CryptoPulse - News Ingestion Function

Timer-triggered Azure Function that crawls cryptocurrency news
from multiple sources and publishes to Kafka / Event Hubs.

Design decisions:
  - Reuses NewsCrawlerOrchestrator from cryptopulse.ingestion
  - Deduplicates by content_hash at the function level
  - Rate limits per source to avoid being blocked
  - 60-second crawl interval (news is less time-sensitive than trades)
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="news_ingestion")


# =============================================================================
# Content Hash Deduplication
# =============================================================================

class NewsDeduplicator:
    """
    Deduplication for news articles by content hash.

    Maintains a bounded set of recently-seen hashes. In production,
    this should be backed by Redis for cross-invocation persistence.
    """

    def __init__(self, max_size: int = 5_000):
        self._seen: set[str] = set()
        self._max_size = max_size

    def is_new(self, content_hash: str) -> bool:
        """Check if article is new based on content hash."""
        if not content_hash or content_hash in self._seen:
            return False
        self._seen.add(content_hash)
        if len(self._seen) > self._max_size:
            # Simple eviction: drop half
            to_keep = list(self._seen)[self._max_size // 2 :]
            self._seen = set(to_keep)
        return True


# =============================================================================
# Main Ingestion Function
# =============================================================================

async def ingest_news(
    lookback_minutes: int = 5,
    publish: bool = True,
) -> dict[str, Any]:
    """
    Main news ingestion entry point.

    Crawls all configured news sources, deduplicates,
    and publishes to Kafka/Event Hubs.

    Args:
        lookback_minutes: How far back to look for articles
        publish: Whether to actually publish (False for testing)

    Returns:
        Summary dict with counts
    """
    start_time = time.monotonic()
    dedup = NewsDeduplicator()

    since = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)

    logger.info(
        "starting_news_ingestion",
        since=since.isoformat(),
        lookback_minutes=lookback_minutes,
    )

    # Import and create orchestrator
    try:
        from cryptopulse.ingestion.news_crawler import create_default_orchestrator

        orchestrator = create_default_orchestrator()
    except ImportError as e:
        logger.error("crawler_import_error", error=str(e))
        return {"error": str(e), "articles_crawled": 0}

    # Crawl all sources
    articles = await orchestrator.crawl_all(since=since)

    total_crawled = len(articles)
    logger.info("articles_crawled", count=total_crawled)

    # Deduplicate
    unique_articles = [
        article for article in articles
        if dedup.is_new(article.content_hash)
    ]

    total_unique = len(unique_articles)
    total_duplicates = total_crawled - total_unique

    logger.info(
        "deduplication_complete",
        unique=total_unique,
        duplicates=total_duplicates,
    )

    # Publish
    total_published = 0
    if publish and unique_articles:
        try:
            from cryptopulse.ingestion.kafka_producer import NewsEventPublisher

            publisher = NewsEventPublisher()

            for article in unique_articles:
                try:
                    publisher.publish(
                        topic=settings.kafka.news_topic,
                        key=article.source,
                        value=article.model_dump(),
                    )
                    total_published += 1
                except Exception as e:
                    logger.warning(
                        "article_publish_error",
                        article_id=article.article_id,
                        error=str(e),
                    )

            publisher.flush()

        except ImportError as e:
            logger.warning("publisher_unavailable", error=str(e))
        except Exception as e:
            logger.error("publish_error", error=str(e))

    elapsed = time.monotonic() - start_time

    # Per-source breakdown
    source_counts: dict[str, int] = {}
    for article in unique_articles:
        source_counts[article.source] = source_counts.get(article.source, 0) + 1

    summary = {
        "total_crawled": total_crawled,
        "total_unique": total_unique,
        "total_duplicates": total_duplicates,
        "total_published": total_published,
        "source_counts": source_counts,
        "elapsed_seconds": round(elapsed, 3),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    logger.info("news_ingestion_complete", **summary)

    return summary


# =============================================================================
# Azure Function / CLI Entry Point
# =============================================================================

def main() -> None:
    """
    Entry point for Azure Functions timer trigger or CLI.
    """
    result = asyncio.run(ingest_news())
    print(f"News ingestion complete: {result}")


if __name__ == "__main__":
    main()
