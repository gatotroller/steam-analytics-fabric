"""
Structured logging configuration using structlog.

Provides consistent, machine-readable logs in production (JSON)
and human-readable logs in development (console).
"""

import logging
import sys
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from structlog.types import Processor

from steam_analytics.config import get_settings


def setup_logging() -> None:
    """
    Configure structured logging for the application.

    Sets up structlog with appropriate processors based on
    the environment (JSON for production, console for development).
    """
    settings = get_settings()

    # Shared processors for all environments
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if settings.logging.include_timestamp:
        shared_processors.insert(0, structlog.processors.TimeStamper(fmt="iso"))

    if settings.logging.format == "json":
        # Production: JSON format for log aggregation
        processors = [
            *shared_processors,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Development: Human-readable console output
        processors = [
            *shared_processors,
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            ),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.logging.level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Also configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.logging.level),
    )


def get_logger(name: str | None = None, **initial_context: Any) -> structlog.BoundLogger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name (typically __name__ of the calling module)
        **initial_context: Initial context values to bind to the logger

    Returns:
        structlog.BoundLogger: Configured logger instance

    Example:
        >>> logger = get_logger(__name__, component="extractor")
        >>> logger.info("Starting extraction", app_id=123)
    """
    logger = structlog.get_logger(name)
    if initial_context:
        logger = logger.bind(**initial_context)
    return logger
