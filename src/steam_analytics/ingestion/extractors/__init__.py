"""
Data extractors for Steam APIs.

This module provides extractors for various Steam APIs,
all built on a common base with retry logic, rate limiting,
and structured logging.
"""

from steam_analytics.ingestion.extractors.base import (
    APIError,
    BaseExtractor,
    ExtractionError,
    ExtractionResult,
    RateLimitError,
    ValidationError,
)
from steam_analytics.ingestion.extractors.steam_player_stats import SteamPlayerStatsExtractor
from steam_analytics.ingestion.extractors.steam_reviews import SteamReviewsExtractor
from steam_analytics.ingestion.extractors.steam_store import SteamStoreExtractor

__all__ = [
    # Base classes and errors
    "APIError",
    "BaseExtractor",
    "ExtractionError",
    "ExtractionResult",
    "RateLimitError",
    "ValidationError",
    # Extractors
    "SteamPlayerStatsExtractor",
    "SteamReviewsExtractor",
    "SteamStoreExtractor",
]
