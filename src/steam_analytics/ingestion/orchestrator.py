"""
Ingestion orchestrator that coordinates all extractors.

Manages the complete ingestion flow from extraction to Bronze storage,
handling batching, error recovery, and progress tracking.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from src.steam_analytics.config import get_settings
from src.steam_analytics.ingestion.bronze.schemas import DataSource
from src.steam_analytics.ingestion.bronze.writer import BronzeWriter
from src.steam_analytics.ingestion.contracts import AppId
from src.steam_analytics.ingestion.extractors import (
    ExtractionResult,
    SteamPlayerStatsExtractor,
    SteamReviewsExtractor,
    SteamStoreExtractor,
)
from src.steam_analytics.logger import get_logger


@dataclass
class IngestionProgress:
    """Tracks progress of an ingestion run."""

    total: int
    completed: int = 0
    successful: int = 0
    failed: int = 0
    current_app_id: int | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def percentage(self) -> float:
        """Get completion percentage."""
        if self.total == 0:
            return 100.0
        return (self.completed / self.total) * 100

    @property
    def elapsed_seconds(self) -> float:
        """Get elapsed time in seconds."""
        return (datetime.now(timezone.utc) - self.started_at).total_seconds()

    @property
    def estimated_remaining_seconds(self) -> float | None:
        """Estimate remaining time based on current pace."""
        if self.completed == 0:
            return None
        pace = self.elapsed_seconds / self.completed
        remaining = self.total - self.completed
        return pace * remaining


@dataclass
class IngestionResult:
    """Result of a complete ingestion run."""

    run_id: UUID
    started_at: datetime
    completed_at: datetime
    total_apps: int
    successful: int
    failed: int
    batches_written: list[str]
    errors: list[dict[str, Any]]

    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_apps == 0:
            return 100.0
        return (self.successful / self.total_apps) * 100

    @property
    def duration_seconds(self) -> float:
        """Get total duration in seconds."""
        return (self.completed_at - self.started_at).total_seconds()


class IngestionOrchestrator:
    """
    Orchestrates the complete ingestion pipeline.

    Coordinates extractors, manages batching, handles errors,
    and writes to Bronze layer.

    Example:
        >>> orchestrator = IngestionOrchestrator()
        >>> result = await orchestrator.run_full_ingestion(
        ...     app_ids=[1091500, 570, 730],
        ...     include_reviews=True,
        ...     include_players=True,
        ... )
        >>> print(f"Success rate: {result.success_rate}%")
    """

    def __init__(
        self,
        *,
        bronze_writer: BronzeWriter | None = None,
        batch_size: int = 100,
    ) -> None:
        """
        Initialize orchestrator.

        Args:
            bronze_writer: Writer for Bronze layer (creates default if None)
            batch_size: Number of records per batch
        """
        self._settings = get_settings()
        self._bronze_writer = bronze_writer or BronzeWriter()
        self._batch_size = batch_size
        self._logger = get_logger(__name__, component="orchestrator")

    async def run_full_ingestion(
        self,
        app_ids: list[AppId],
        *,
        include_store: bool = True,
        include_reviews: bool = True,
        include_players: bool = True,
        on_progress: Callable[[IngestionProgress], None] | None = None,
    ) -> IngestionResult:
        """
        Run complete ingestion for a list of app IDs.

        Args:
            app_ids: List of Steam App IDs to ingest
            include_store: Include Store API data
            include_reviews: Include Reviews API data
            include_players: Include Player Stats API data
            on_progress: Optional callback for progress updates

        Returns:
            IngestionResult: Summary of the ingestion run
        """
        run_id = uuid4()
        started_at = datetime.now(timezone.utc)
        batches_written: list[str] = []
        errors: list[dict[str, Any]] = []

        self._logger.info(
            "Starting full ingestion",
            run_id=str(run_id),
            total_apps=len(app_ids),
            include_store=include_store,
            include_reviews=include_reviews,
            include_players=include_players,
        )

        progress = IngestionProgress(total=len(app_ids))

        # Run ingestion for each source
        if include_store:
            store_result = await self._ingest_source(
                DataSource.STEAM_STORE,
                app_ids,
                progress,
                on_progress,
            )
            batches_written.extend(store_result["batches"])
            errors.extend(store_result["errors"])

        if include_reviews:
            reviews_result = await self._ingest_source(
                DataSource.STEAM_REVIEWS,
                app_ids,
                progress,
                on_progress,
            )
            batches_written.extend(reviews_result["batches"])
            errors.extend(reviews_result["errors"])

        if include_players:
            players_result = await self._ingest_source(
                DataSource.STEAM_PLAYER_STATS,
                app_ids,
                progress,
                on_progress,
            )
            batches_written.extend(players_result["batches"])
            errors.extend(players_result["errors"])

        completed_at = datetime.now(timezone.utc)

        result = IngestionResult(
            run_id=run_id,
            started_at=started_at,
            completed_at=completed_at,
            total_apps=len(app_ids),
            successful=progress.successful,
            failed=progress.failed,
            batches_written=batches_written,
            errors=errors,
        )

        self._logger.info(
            "Ingestion complete",
            run_id=str(run_id),
            duration_seconds=result.duration_seconds,
            success_rate=result.success_rate,
            batches_written=len(batches_written),
            total_errors=len(errors),
        )

        return result

    async def _ingest_source(
        self,
        source: DataSource,
        app_ids: list[AppId],
        progress: IngestionProgress,
        on_progress: Callable[[IngestionProgress], None] | None,
    ) -> dict[str, Any]:
        """Ingest data from a specific source."""
        batches: list[str] = []
        errors: list[dict[str, Any]] = []

        self._logger.info(
            "Starting source ingestion",
            source=source.value,
            total_apps=len(app_ids),
        )

        # Create extractor based on source
        extractor = self._create_extractor(source)

        # Process in batches
        for batch_start in range(0, len(app_ids), self._batch_size):
            batch_app_ids = app_ids[batch_start : batch_start + self._batch_size]

            batch = self._bronze_writer.create_batch(source, total_requested=len(batch_app_ids))

            async with extractor:
                for app_id in batch_app_ids:
                    progress.current_app_id = app_id

                    try:
                        result = await self._extract_single(extractor, source, app_id)

                        self._bronze_writer.add_to_batch(
                            batch, result, request_params={"app_id": app_id}
                        )

                        if result.success:
                            progress.successful += 1
                        else:
                            progress.failed += 1
                            errors.append(
                                {
                                    "source": source.value,
                                    "app_id": app_id,
                                    "error": result.error_message,
                                }
                            )

                    except Exception as e:
                        self._logger.exception(
                            "Extraction error",
                            source=source.value,
                            app_id=app_id,
                            error=str(e),
                        )
                        progress.failed += 1
                        errors.append(
                            {
                                "source": source.value,
                                "app_id": app_id,
                                "error": str(e),
                            }
                        )

                    progress.completed += 1

                    if on_progress:
                        on_progress(progress)

            # Write batch
            batch_path = self._bronze_writer.write_batch(batch)
            batches.append(str(batch_path))

        return {"batches": batches, "errors": errors}

    def _create_extractor(self, source: DataSource) -> Any:
        """Create appropriate extractor for source."""
        if source == DataSource.STEAM_STORE:
            return SteamStoreExtractor()
        elif source == DataSource.STEAM_REVIEWS:
            return SteamReviewsExtractor()
        elif source == DataSource.STEAM_PLAYER_STATS:
            return SteamPlayerStatsExtractor()
        else:
            raise ValueError(f"Unknown source: {source}")

    async def _extract_single(
        self,
        extractor: Any,
        source: DataSource,
        app_id: AppId,
    ) -> ExtractionResult:
        """Extract single record from appropriate extractor."""
        if source == DataSource.STEAM_STORE:
            return await extractor.extract(app_id=app_id)
        elif source == DataSource.STEAM_REVIEWS:
            return await extractor.extract_summary_only(app_id=app_id)
        elif source == DataSource.STEAM_PLAYER_STATS:
            return await extractor.extract(app_id=app_id)
        else:
            raise ValueError(f"Unknown source: {source}")

    async def run_single_game(self, app_id: AppId) -> dict[str, ExtractionResult]:
        """
        Convenience method to extract all data for a single game.

        Args:
            app_id: Steam App ID

        Returns:
            dict: Results from each source
        """
        results = {}

        async with SteamStoreExtractor() as extractor:
            results["store"] = await extractor.extract(app_id=app_id)

        async with SteamReviewsExtractor() as extractor:
            results["reviews"] = await extractor.extract_summary_only(app_id=app_id)

        async with SteamPlayerStatsExtractor() as extractor:
            results["players"] = await extractor.extract(app_id=app_id)

        return results
