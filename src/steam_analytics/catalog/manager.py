"""
Game Catalog Manager.

Manages the catalog of games to track, including classification and prioritization
based on player counts from official Steam APIs.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

import structlog

logger = structlog.get_logger(__name__)


class SyncPriority(str, Enum):
    """
    Sync priority levels based on player activity.

    Determines how frequently a game's data is refreshed.
    """

    HIGH = "high"  # Daily sync - Popular games (1000+ players)
    MEDIUM = "medium"  # Weekly sync - Active games (100+ players)
    LOW = "low"  # Monthly sync - Low activity (1+ players)
    SKIP = "skip"  # Never sync - Dead/invalid games (0 players)


@dataclass
class CatalogEntry:
    """A game entry in the catalog."""

    app_id: int
    name: str
    player_count: int | None
    priority: SyncPriority
    discovered_at: datetime
    last_synced_at: datetime | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame creation."""
        return {
            "app_id": self.app_id,
            "name": self.name,
            "player_count": self.player_count,
            "priority": self.priority.value,
            "discovered_at": self.discovered_at.isoformat(),
            "last_synced_at": self.last_synced_at.isoformat() if self.last_synced_at else None,
        }


class GameCatalogManager:
    """
    Manages the game catalog with smart prioritization.

    Priority is determined by current player count:
    - HIGH:   >= 1,000 players  (top ~2,000 games)
    - MEDIUM: >= 100 players    (top ~10,000 games)
    - LOW:    >= 1 player       (active games ~30,000)
    - SKIP:   0 players         (dead/broken games)
    """

    # Player count thresholds for priority
    THRESHOLD_HIGH = 1000
    THRESHOLD_MEDIUM = 100
    THRESHOLD_LOW = 1

    # Staleness thresholds (days since last sync)
    STALE_DAYS_HIGH = 1  # HIGH priority: stale after 1 day
    STALE_DAYS_MEDIUM = 7  # MEDIUM priority: stale after 7 days
    STALE_DAYS_LOW = 30  # LOW priority: stale after 30 days

    def calculate_priority(self, player_count: int | None) -> SyncPriority:
        """
        Calculate sync priority based on current player count.

        Args:
            player_count: Current number of players (None = unknown)

        Returns:
            SyncPriority level
        """
        if player_count is None:
            # Unknown player count - default to LOW to check later
            return SyncPriority.LOW

        if player_count >= self.THRESHOLD_HIGH:
            return SyncPriority.HIGH
        elif player_count >= self.THRESHOLD_MEDIUM:
            return SyncPriority.MEDIUM
        elif player_count >= self.THRESHOLD_LOW:
            return SyncPriority.LOW
        else:
            return SyncPriority.SKIP

    def create_catalog_entry(
        self,
        app_id: int,
        name: str,
        player_count: int | None,
        discovered_at: datetime | None = None,
    ) -> CatalogEntry:
        """
        Create a catalog entry with calculated priority.

        Args:
            app_id: Steam app ID
            name: Game name
            player_count: Current player count
            discovered_at: When the game was discovered (default: now)

        Returns:
            CatalogEntry with priority set
        """
        priority = self.calculate_priority(player_count)

        return CatalogEntry(
            app_id=app_id,
            name=name,
            player_count=player_count,
            priority=priority,
            discovered_at=discovered_at or datetime.now(timezone.utc),
        )

    def get_apps_to_sync(
        self,
        catalog: list[CatalogEntry],
        priorities: list[SyncPriority],
        check_staleness: bool = True,
    ) -> list[int]:
        """
        Get app IDs that need syncing based on priority and staleness.

        Args:
            catalog: Full catalog of games
            priorities: List of priorities to include
            check_staleness: If True, only return stale entries

        Returns:
            List of app_ids to sync
        """
        now = datetime.now(timezone.utc)
        apps_to_sync = []

        for entry in catalog:
            # Skip if not in requested priorities
            if entry.priority not in priorities:
                continue

            # Skip if priority is SKIP
            if entry.priority == SyncPriority.SKIP:
                continue

            # Check staleness if requested
            if check_staleness and entry.last_synced_at:
                stale_days = self._get_stale_days(entry.priority)
                stale_threshold = now - timedelta(days=stale_days)

                if entry.last_synced_at > stale_threshold:
                    continue  # Not stale yet, skip

            apps_to_sync.append(entry.app_id)

        logger.info(
            "Calculated apps to sync",
            priorities=[p.value for p in priorities],
            total_in_catalog=len(catalog),
            apps_to_sync=len(apps_to_sync),
        )

        return apps_to_sync

    def _get_stale_days(self, priority: SyncPriority) -> int:
        """Get staleness threshold in days for a priority level."""
        return {
            SyncPriority.HIGH: self.STALE_DAYS_HIGH,
            SyncPriority.MEDIUM: self.STALE_DAYS_MEDIUM,
            SyncPriority.LOW: self.STALE_DAYS_LOW,
            SyncPriority.SKIP: 999999,  # Never stale
        }[priority]

    def get_sync_schedule(self, day_of_week: int) -> list[SyncPriority]:
        """
        Get which priorities to sync based on day of week.

        Schedule:
        - Monday (0):    HIGH + MEDIUM
        - Tue-Fri (1-4): HIGH only
        - Saturday (5):  HIGH + MEDIUM
        - Sunday (6):    HIGH + MEDIUM + LOW (sample)

        Args:
            day_of_week: 0=Monday, 6=Sunday

        Returns:
            List of priorities to sync
        """
        if day_of_week == 0:  # Monday
            return [SyncPriority.HIGH, SyncPriority.MEDIUM]
        elif day_of_week in [1, 2, 3, 4]:  # Tue-Fri
            return [SyncPriority.HIGH]
        elif day_of_week == 5:  # Saturday
            return [SyncPriority.HIGH, SyncPriority.MEDIUM]
        else:  # Sunday
            return [SyncPriority.HIGH, SyncPriority.MEDIUM, SyncPriority.LOW]

    def get_catalog_stats(self, catalog: list[CatalogEntry]) -> dict:
        """
        Get statistics about the catalog.

        Args:
            catalog: Full catalog

        Returns:
            Dict with counts per priority
        """
        stats = {
            "total": len(catalog),
            "by_priority": {
                SyncPriority.HIGH.value: 0,
                SyncPriority.MEDIUM.value: 0,
                SyncPriority.LOW.value: 0,
                SyncPriority.SKIP.value: 0,
            },
            "with_players": 0,
            "total_players": 0,
        }

        for entry in catalog:
            stats["by_priority"][entry.priority.value] += 1

            if entry.player_count and entry.player_count > 0:
                stats["with_players"] += 1
                stats["total_players"] += entry.player_count

        return stats

    def estimate_sync_time(
        self,
        app_count: int,
        sources: int = 3,
        requests_per_second: float = 10.0,
    ) -> dict:
        """
        Estimate time to sync a given number of apps.

        Args:
            app_count: Number of apps to sync
            sources: Number of API sources (store, reviews, players)
            requests_per_second: API rate limit

        Returns:
            Dict with time estimates
        """
        total_requests = app_count * sources
        seconds = total_requests / requests_per_second
        minutes = seconds / 60
        hours = minutes / 60

        return {
            "app_count": app_count,
            "total_requests": total_requests,
            "estimated_seconds": round(seconds, 1),
            "estimated_minutes": round(minutes, 1),
            "estimated_hours": round(hours, 2),
        }
