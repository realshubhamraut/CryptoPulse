"""
CryptoPulse - News Crawler Framework

Multi-source news crawler with deduplication and rate limiting.
"""

import asyncio
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any

import httpx
from pydantic import BaseModel, Field

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="news_crawler")


class NewsArticle(BaseModel):
    """Standardized news article format."""
    
    source: str
    article_id: str
    title: str
    content: str
    url: str
    published_at: datetime
    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    author: str | None = None
    tags: list[str] = Field(default_factory=list)
    content_hash: str = ""
    
    def model_post_init(self, __context: Any) -> None:
        """Generate content hash after initialization."""
        if not self.content_hash:
            hash_input = f"{self.title}:{self.content[:1000]}"
            self.content_hash = hashlib.sha256(hash_input.encode()).hexdigest()


class BaseCrawler(ABC):
    """Abstract base class for news crawlers."""
    
    name: str = "base"
    base_url: str = ""
    rate_limit_per_minute: int = 30
    
    def __init__(self):
        self._client: httpx.AsyncClient | None = None
        self._last_request_time: float = 0
        self._request_interval = 60.0 / self.rate_limit_per_minute
        self._seen_hashes: set[str] = set()
    
    async def __aenter__(self) -> "BaseCrawler":
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": "CryptoPulse/1.0 (Market Intelligence Bot)",
                "Accept": "application/json",
            },
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            await self._client.aclose()
    
    async def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self._request_interval:
            await asyncio.sleep(self._request_interval - elapsed)
        self._last_request_time = asyncio.get_event_loop().time()
    
    async def _get(self, url: str, **kwargs) -> httpx.Response:
        """Make rate-limited GET request."""
        await self._rate_limit()
        return await self._client.get(url, **kwargs)
    
    def _deduplicate(self, articles: list[NewsArticle]) -> list[NewsArticle]:
        """Remove duplicate articles based on content hash."""
        unique = []
        for article in articles:
            if article.content_hash not in self._seen_hashes:
                self._seen_hashes.add(article.content_hash)
                unique.append(article)
        return unique
    
    @abstractmethod
    async def fetch_articles(self, since: datetime | None = None) -> list[NewsArticle]:
        """
        Fetch articles from the source.
        
        Args:
            since: Only fetch articles published after this time
            
        Returns:
            List of NewsArticle objects
        """
        pass


class CoinDeskCrawler(BaseCrawler):
    """CoinDesk news crawler."""
    
    name = "coindesk"
    base_url = "https://www.coindesk.com"
    
    async def fetch_articles(self, since: datetime | None = None) -> list[NewsArticle]:
        """Fetch latest articles from CoinDesk."""
        articles = []
        
        try:
            # CoinDesk API endpoint (simplified - actual API may differ)
            url = f"{self.base_url}/pf/api/v3/content/fetch/articles"
            params = {
                "query": '{"size":20}',
                "_website": "coindesk",
            }
            
            response = await self._get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                for item in data.get("content_elements", []):
                    try:
                        pub_date = datetime.fromisoformat(
                            item.get("display_date", "").replace("Z", "+00:00")
                        )
                        
                        if since and pub_date < since:
                            continue
                        
                        article = NewsArticle(
                            source=self.name,
                            article_id=item.get("_id", ""),
                            title=item.get("headlines", {}).get("basic", ""),
                            content=item.get("description", {}).get("basic", ""),
                            url=f"{self.base_url}{item.get('canonical_url', '')}",
                            published_at=pub_date,
                            author=item.get("credits", {}).get("by", [{}])[0].get("name"),
                            tags=[t.get("text", "") for t in item.get("taxonomy", {}).get("tags", [])],
                        )
                        articles.append(article)
                    except Exception as e:
                        logger.warning("article_parse_error", source=self.name, error=str(e))
                        
            else:
                logger.warning(
                    "coindesk_fetch_error",
                    status=response.status_code,
                )
                
        except Exception as e:
            logger.error("coindesk_crawler_error", error=str(e))
        
        return self._deduplicate(articles)


class CoinTelegraphCrawler(BaseCrawler):
    """CoinTelegraph news crawler."""
    
    name = "cointelegraph"
    base_url = "https://cointelegraph.com"
    
    async def fetch_articles(self, since: datetime | None = None) -> list[NewsArticle]:
        """Fetch latest articles from CoinTelegraph."""
        articles = []
        
        try:
            # CoinTelegraph RSS/API endpoint
            url = f"{self.base_url}/rss"
            
            response = await self._get(url)
            
            if response.status_code == 200:
                # Parse RSS feed (simplified)
                import xml.etree.ElementTree as ET
                
                root = ET.fromstring(response.text)
                
                for item in root.findall(".//item"):
                    try:
                        pub_date_str = item.find("pubDate").text
                        pub_date = datetime.strptime(
                            pub_date_str, "%a, %d %b %Y %H:%M:%S %z"
                        )
                        
                        if since and pub_date < since:
                            continue
                        
                        article = NewsArticle(
                            source=self.name,
                            article_id=item.find("guid").text or "",
                            title=item.find("title").text or "",
                            content=item.find("description").text or "",
                            url=item.find("link").text or "",
                            published_at=pub_date,
                        )
                        articles.append(article)
                    except Exception as e:
                        logger.warning("article_parse_error", source=self.name, error=str(e))
            else:
                logger.warning(
                    "cointelegraph_fetch_error",
                    status=response.status_code,
                )
                
        except Exception as e:
            logger.error("cointelegraph_crawler_error", error=str(e))
        
        return self._deduplicate(articles)


class CryptoCompareCrawler(BaseCrawler):
    """CryptoCompare news API crawler."""
    
    name = "cryptocompare"
    base_url = "https://min-api.cryptocompare.com/data/v2/news/"
    
    def __init__(self, api_key: str | None = None):
        super().__init__()
        self.api_key = api_key
    
    async def fetch_articles(self, since: datetime | None = None) -> list[NewsArticle]:
        """Fetch latest articles from CryptoCompare."""
        articles = []
        
        try:
            params = {"lang": "EN"}
            if self.api_key:
                params["api_key"] = self.api_key
            
            response = await self._get(self.base_url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                for item in data.get("Data", []):
                    try:
                        pub_date = datetime.fromtimestamp(item.get("published_on", 0))
                        
                        if since and pub_date < since:
                            continue
                        
                        article = NewsArticle(
                            source=self.name,
                            article_id=str(item.get("id", "")),
                            title=item.get("title", ""),
                            content=item.get("body", ""),
                            url=item.get("url", ""),
                            published_at=pub_date,
                            tags=item.get("categories", "").split("|"),
                        )
                        articles.append(article)
                    except Exception as e:
                        logger.warning("article_parse_error", source=self.name, error=str(e))
            else:
                logger.warning(
                    "cryptocompare_fetch_error",
                    status=response.status_code,
                )
                
        except Exception as e:
            logger.error("cryptocompare_crawler_error", error=str(e))
        
        return self._deduplicate(articles)


class NewsCrawlerOrchestrator:
    """
    Orchestrates multiple news crawlers.
    
    Manages parallel crawling, deduplication across sources,
    and publishing to message queue.
    """
    
    def __init__(self, crawlers: list[BaseCrawler] | None = None):
        self.crawlers = crawlers or []
        self._global_seen_hashes: set[str] = set()
        self._last_crawl_time: datetime | None = None
    
    def add_crawler(self, crawler: BaseCrawler) -> None:
        """Add a crawler to the orchestrator."""
        self.crawlers.append(crawler)
    
    async def crawl_all(
        self,
        since: datetime | None = None,
    ) -> list[NewsArticle]:
        """
        Crawl all sources in parallel.
        
        Args:
            since: Only fetch articles after this time
            
        Returns:
            Deduplicated list of articles from all sources
        """
        if since is None and self._last_crawl_time:
            since = self._last_crawl_time - timedelta(minutes=5)  # Overlap for safety
        
        all_articles: list[NewsArticle] = []
        
        async def crawl_source(crawler: BaseCrawler) -> list[NewsArticle]:
            async with crawler:
                return await crawler.fetch_articles(since)
        
        # Crawl all sources concurrently
        results = await asyncio.gather(
            *[crawl_source(c) for c in self.crawlers],
            return_exceptions=True,
        )
        
        for result in results:
            if isinstance(result, Exception):
                logger.error("crawler_failed", error=str(result))
            else:
                all_articles.extend(result)
        
        # Global deduplication
        unique_articles = []
        for article in all_articles:
            if article.content_hash not in self._global_seen_hashes:
                self._global_seen_hashes.add(article.content_hash)
                unique_articles.append(article)
        
        self._last_crawl_time = datetime.utcnow()
        
        logger.info(
            "crawl_complete",
            total_articles=len(all_articles),
            unique_articles=len(unique_articles),
            sources=len(self.crawlers),
        )
        
        return unique_articles
    
    async def run_continuous(
        self,
        interval_seconds: int = 60,
        callback=None,
    ) -> None:
        """
        Run continuous crawling loop.
        
        Args:
            interval_seconds: Time between crawl cycles
            callback: Async function to call with each batch of articles
        """
        logger.info("starting_continuous_crawl", interval=interval_seconds)
        
        while True:
            try:
                articles = await self.crawl_all()
                
                if articles and callback:
                    await callback(articles)
                    
            except Exception as e:
                logger.error("crawl_cycle_error", error=str(e))
            
            await asyncio.sleep(interval_seconds)


def create_default_orchestrator() -> NewsCrawlerOrchestrator:
    """Create orchestrator with default crawlers."""
    orchestrator = NewsCrawlerOrchestrator()
    
    orchestrator.add_crawler(CoinDeskCrawler())
    orchestrator.add_crawler(CoinTelegraphCrawler())
    orchestrator.add_crawler(CryptoCompareCrawler())
    
    return orchestrator


if __name__ == "__main__":
    # Quick test
    async def main():
        orchestrator = create_default_orchestrator()
        articles = await orchestrator.crawl_all()
        
        for article in articles[:5]:
            print(f"[{article.source}] {article.title[:60]}...")
    
    asyncio.run(main())
