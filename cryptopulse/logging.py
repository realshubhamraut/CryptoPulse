"""
CryptoPulse - Structured Logging Configuration

Uses structlog for structured, JSON-formatted logging with context.
"""

import logging
import sys
from typing import Any

import structlog
from structlog.typing import Processor

from cryptopulse.config import settings


def setup_logging() -> None:
    """Configure structured logging for the application."""
    
    # Determine log level
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    
    # Shared processors for all loggers
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]
    
    if settings.debug:
        # Development: colorful console output
        processors: list[Processor] = [
            *shared_processors,
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    else:
        # Production: JSON output
        processors = [
            *shared_processors,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Also configure standard library logging for third-party libs
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )
    
    # Suppress noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)


def get_logger(name: str | None = None, **initial_context: Any) -> structlog.BoundLogger:
    """
    Get a bound logger with optional initial context.
    
    Args:
        name: Logger name (usually __name__)
        **initial_context: Initial context key-value pairs
        
    Returns:
        Bound logger instance
    """
    logger = structlog.get_logger(name)
    if initial_context:
        logger = logger.bind(**initial_context)
    return logger


# Pre-configured loggers for common components
def get_ingestion_logger() -> structlog.BoundLogger:
    """Get logger for ingestion components."""
    return get_logger("cryptopulse.ingestion", component="ingestion")


def get_pipeline_logger() -> structlog.BoundLogger:
    """Get logger for pipeline components."""
    return get_logger("cryptopulse.pipeline", component="pipeline")


def get_ml_logger() -> structlog.BoundLogger:
    """Get logger for ML components."""
    return get_logger("cryptopulse.ml", component="ml")


def get_api_logger() -> structlog.BoundLogger:
    """Get logger for API components."""
    return get_logger("cryptopulse.api", component="api")
