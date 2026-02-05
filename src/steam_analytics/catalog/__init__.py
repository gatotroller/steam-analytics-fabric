"""
Game Catalog Management.

Handles game discovery, classification, and prioritization
for efficient data extraction scheduling.
"""

from src.steam_analytics.catalog.manager import (
    CatalogEntry,
    GameCatalogManager,
    SyncPriority,
)

__all__ = [
    "GameCatalogManager",
    "CatalogEntry",
    "SyncPriority",
]
