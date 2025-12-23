"""
Data contracts for Steam API responses.

This module provides Pydantic models that define the expected
structure of data from various Steam APIs, ensuring type safety
and validation throughout the ingestion pipeline.
"""

from steam_analytics.ingestion.contracts.steam_player_stats import (
    PlayerCountAPIResponse,
    PlayerCountResponse,
)
from steam_analytics.ingestion.contracts.steam_reviews import (
    Review,
    ReviewAuthor,
    ReviewQuerySummary,
    SteamReviewsResponse,
)
from steam_analytics.ingestion.contracts.steam_store import (
    AppId,
    Category,
    Genre,
    Metacritic,
    Platform,
    PriceOverview,
    ReleaseDate,
    Screenshot,
    SteamStoreAPIResponse,
    SteamStoreGame,
)

__all__ = [
    "AppId",
    "Category",
    "Genre",
    "Metacritic",
    "Platform",
    "PlayerCountAPIResponse",
    "PlayerCountResponse",
    "PriceOverview",
    "ReleaseDate",
    "Review",
    "ReviewAuthor",
    "ReviewQuerySummary",
    "Screenshot",
    "SteamReviewsResponse",
    "SteamStoreAPIResponse",
    "SteamStoreGame",
]
