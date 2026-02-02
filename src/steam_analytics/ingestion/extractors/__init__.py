"""
Data extractors for Steam APIs.

Exports the main extractor classes and exceptions for easy access.
Each extractor handles a specific endpoint (Store, Reviews, Players)
and encapsulates logic for:
- Rate limiting
- Validation
- Error handling
- Data parsing
"""

from src.steam_analytics.ingestion.extractors.base import (
    APIError,
    BaseExtractor,
    ExtractionError,
    ExtractionResult,
    RateLimitError,
    ValidationError,
)
from src.steam_analytics.ingestion.extractors.steam_player_stats import SteamPlayerStatsExtractor
from src.steam_analytics.ingestion.extractors.steam_reviews import SteamReviewsExtractor
from src.steam_analytics.ingestion.extractors.steam_store import SteamStoreExtractor

__all__ = [
    "APIError",
    "BaseExtractor",
    "ExtractionError",
    "ExtractionResult",
    "RateLimitError",
    "ValidationError",
    "SteamPlayerStatsExtractor",
    "SteamReviewsExtractor",
    "SteamStoreExtractor",
]
