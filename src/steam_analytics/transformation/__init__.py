"""
Transformation modules for Medallion Architecture.

All transformation logic lives here as reusable Python functions.
Notebooks just import and call these functions.
"""

from src.steam_analytics.transformation.gold import (
    build_game_metrics,
    build_genre_summary,
    build_price_history,
    run_gold_transform,
)
from src.steam_analytics.transformation.silver import (
    run_silver_transform,
    transform_games,
    transform_player_counts,
    transform_reviews,
)

__all__ = [
    # Silver
    "run_silver_transform",
    "transform_games",
    "transform_reviews",
    "transform_player_counts",
    # Gold
    "run_gold_transform",
    "build_game_metrics",
    "build_price_history",
    "build_genre_summary",
]
