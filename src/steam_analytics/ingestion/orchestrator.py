"""
Ingestion orchestrator that coordinates all extractors.

Manages the complete ingestion flow from extraction to Bronze storage,
handling batching, error recovery, and progress tracking.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from src.steam_analytics.config import get_settings
from src.steam_analytics.ingestion.bronze.onelake_writer import OneLakeWriter
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


# 1. RESTAURAMOS LA CLASE OUTPUTTARGET
class OutputTarget(str, Enum):
    """Output target for ingestion."""

    LOCAL = "local"
    ONELAKE = "onelake"


@dataclass
class IngestionProgress:
    """Tracks progress of an ingestion run."""

    total: int
    completed: int = 0
    successful: int = 0
    failed: int = 0
    current_app_id: int | None = None
    current_source: str | None = None
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


@dataclass
class IngestionResult:
    """Result of a complete ingestion run."""

    run_id: UUID
    started_at: datetime
    completed_at: datetime
    total_apps: int
    expected_total_records: int
    successful: int
    failed: int
    batches_written: list[str]
    errors: list[dict[str, Any]]

    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage based on OPERATIONS."""
        if self.expected_total_records == 0:
            return 100.0
        return (self.successful / self.expected_total_records) * 100

    @property
    def duration_seconds(self) -> float:
        """Get total duration in seconds."""
        return (self.completed_at - self.started_at).total_seconds()


class IngestionOrchestrator:
    """
    Orchestrates the complete ingestion pipeline.
    """

    def __init__(
        self,
        *,
        target: OutputTarget = OutputTarget.LOCAL,  # <--- RECIBE EL TARGET EXPLÍCITO
        batch_size: int = 100,
    ) -> None:
        self._settings = get_settings()
        self._batch_size = batch_size
        self._logger = get_logger(__name__, component="orchestrator")

        # 2. LÓGICA DE SELECCIÓN BASADA EN EL ARGUMENTO TARGET
        self._bronze_writer: BronzeWriter | OneLakeWriter

        if target == OutputTarget.ONELAKE:
            self._logger.info("Initializing OneLakeWriter (Explicit Target)")
            self._bronze_writer = OneLakeWriter()
        else:
            self._logger.info("Initializing BronzeWriter (Local Target)")
            self._bronze_writer = BronzeWriter()

    async def run(
        self,
        app_ids: list[AppId],
        *,
        include_store: bool = True,
        include_reviews: bool = True,
        include_players: bool = True,
        on_progress: Callable[[IngestionProgress], None] | None = None,
    ) -> IngestionResult:
        """Alias for run_full_ingestion compatible with CLI."""
        return await self.run_full_ingestion(
            app_ids=app_ids,
            include_store=include_store,
            include_reviews=include_reviews,
            include_players=include_players,
            on_progress=on_progress,
        )

    async def run_full_ingestion(
        self,
        app_ids: list[AppId],
        *,
        include_store: bool = True,
        include_reviews: bool = True,
        include_players: bool = True,
        on_progress: Callable[[IngestionProgress], None] | None = None,
    ) -> IngestionResult:
        """Run complete ingestion for a list of app IDs."""
        run_id = uuid4()
        started_at = datetime.now(timezone.utc)
        batches_written: list[str] = []
        errors: list[dict[str, Any]] = []

        self._logger.info(
            "Starting ingestion",
            run_id=str(run_id),
            total_apps=len(app_ids),
        )

        active_sources_count = sum([include_store, include_reviews, include_players])
        total_operations = len(app_ids) * active_sources_count

        progress = IngestionProgress(total=total_operations)

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
            expected_total_records=total_operations,
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
        """Ingest data from a specific source with PARALLEL execution."""
        batches: list[str] = []
        errors: list[dict[str, Any]] = []

        progress.current_source = source.value

        self._logger.info(f"Processing source: {source.value}")

        extractor = self._create_extractor(source)

        for batch_start in range(0, len(app_ids), self._batch_size):
            batch_app_ids = app_ids[batch_start : batch_start + self._batch_size]

            # Nota: create_batch y add_to_batch funcionan igual en local y OneLake
            # gracias a que replicamos la interfaz
            batch = self._bronze_writer.create_batch(source, total_requested=len(batch_app_ids))

            async with extractor:
                tasks = []
                for app_id in batch_app_ids:
                    task = self._extract_single(extractor, source, app_id)
                    tasks.append(task)

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for i, result in enumerate(results):
                    app_id = batch_app_ids[i]
                    progress.current_app_id = app_id

                    if isinstance(result, Exception):
                        self._logger.error(f"Error extracting {app_id}: {result}")
                        progress.failed += 1
                        errors.append(
                            {"source": source.value, "app_id": app_id, "error": str(result)}
                        )
                        continue

                    try:
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
                        progress.failed += 1
                        errors.append({"source": source.value, "app_id": app_id, "error": str(e)})

                    progress.completed += 1

                    if on_progress:
                        on_progress(progress)

            batch_path = self._bronze_writer.write_batch(batch)
            batches.append(str(batch_path))

        return {"batches": batches, "errors": errors}

    def _create_extractor(self, source: DataSource) -> Any:
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
    ) -> ExtractionResult[Any]:
        if source == DataSource.STEAM_STORE:
            return await extractor.extract(app_id=app_id)
        elif source == DataSource.STEAM_REVIEWS:
            return await extractor.extract_summary_only(app_id=app_id)
        elif source == DataSource.STEAM_PLAYER_STATS:
            return await extractor.extract(app_id=app_id)
        else:
            raise ValueError(f"Unknown source: {source}")
