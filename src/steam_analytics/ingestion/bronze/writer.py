"""
Bronze layer writer for persisting raw data.

Handles writing raw API responses to the Bronze layer
with proper metadata, partitioning, and error handling.
"""

import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from src.steam_analytics.config import get_settings
from src.steam_analytics.ingestion.bronze.schemas import (
    BRONZE_TABLES,
    BronzeBatch,
    BronzeMetadata,
    BronzeRecord,
    BronzeTableConfig,
    DataSource,
    IngestionStatus,
)
from src.steam_analytics.ingestion.extractors.base import ExtractionResult
from src.steam_analytics.logger import get_logger


class BronzeWriter:
    """
    Writes raw data to the Bronze layer.

    In local development, writes to JSON files.
    In Fabric, would write to Delta Lake tables.

    Example:
        >>> writer = BronzeWriter()
        >>> batch = writer.create_batch(DataSource.STEAM_STORE, app_ids=[1091500, 570])
        >>> writer.write_result(batch, extraction_result, app_id=1091500)
        >>> writer.complete_batch(batch)
    """

    def __init__(
        self,
        *,
        output_dir: Path | None = None,
    ) -> None:
        """
        Initialize Bronze writer.

        Args:
            output_dir: Directory for local output (defaults to ./data/bronze)
        """
        self._settings = get_settings()
        self._output_dir = output_dir or Path("data/bronze")
        self._logger = get_logger(__name__, component="bronze_writer")

        # Create output directory if needed
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def create_batch(
        self,
        source: DataSource,
        *,
        total_requested: int = 0,
        batch_id: UUID | None = None,
    ) -> BronzeBatch:
        """
        Create a new ingestion batch.

        Args:
            source: Data source for this batch
            total_requested: Number of items to be ingested
            batch_id: Optional batch ID (generates new if None)

        Returns:
            BronzeBatch: New batch for tracking records
        """
        batch = BronzeBatch(
            batch_id=batch_id or uuid4(),
            source=source,
            total_requested=total_requested,
        )

        self._logger.info(
            "Created new batch",
            batch_id=str(batch.batch_id),
            source=source.value,
            total_requested=total_requested,
        )

        return batch

    def extraction_to_record(
        self,
        batch: BronzeBatch,
        result: ExtractionResult[Any],
        *,
        request_params: dict[str, Any] | None = None,
    ) -> BronzeRecord:
        """
        Convert an extraction result to a Bronze record.

        Args:
            batch: Parent batch for this record
            result: Extraction result from an extractor
            request_params: Original request parameters

        Returns:
            BronzeRecord: Record ready for storage
        """
        metadata = BronzeMetadata(
            batch_id=batch.batch_id,
            source=batch.source,
            source_endpoint=result.endpoint,
            ingested_at=result.extracted_at,
            api_response_time_ms=result.duration_ms,
            status=IngestionStatus.SUCCESS if result.success else IngestionStatus.FAILED,
            error_message=result.error_message,
            request_params=request_params or {},
            environment=self._settings.environment,
        )

        # Store raw response or error info
        if result.success and result.data:
            raw_data = result.data.model_dump()
        elif result.raw_response:
            raw_data = result.raw_response
        else:
            raw_data = {"error": result.error_message}

        return BronzeRecord(metadata=metadata, raw_data=raw_data)

    def add_to_batch(
        self,
        batch: BronzeBatch,
        result: ExtractionResult[Any],
        *,
        request_params: dict[str, Any] | None = None,
    ) -> BronzeRecord:
        """
        Add an extraction result to a batch.

        Args:
            batch: Batch to add record to
            result: Extraction result
            request_params: Original request parameters

        Returns:
            BronzeRecord: The created record
        """
        record = self.extraction_to_record(batch, result, request_params=request_params)
        batch.add_record(record)

        self._logger.debug(
            "Added record to batch",
            batch_id=str(batch.batch_id),
            record_id=str(record.metadata.record_id),
            status=record.metadata.status.value,
        )

        return record

    def write_batch(self, batch: BronzeBatch) -> Path:
        """
        Write a complete batch to storage.

        In local mode, writes to a JSON file.
        In Fabric mode, would write to Delta Lake.

        Args:
            batch: Batch to write

        Returns:
            Path: Path where data was written
        """
        # Mark batch complete
        batch.complete()

        # Get table config
        table_config = BRONZE_TABLES[batch.source]

        # Create partition path based on ingestion date
        ingestion_date = batch.started_at.strftime("%Y-%m-%d")
        partition_path = self._output_dir / table_config.table_name / f"date={ingestion_date}"
        partition_path.mkdir(parents=True, exist_ok=True)

        # Write batch file
        filename = f"batch_{batch.batch_id}_{batch.started_at.strftime('%H%M%S')}.json"
        output_path = partition_path / filename

        # Prepare data for writing
        batch_data = {
            "batch_id": str(batch.batch_id),
            "source": batch.source.value,
            "started_at": batch.started_at.isoformat(),
            "completed_at": batch.completed_at.isoformat() if batch.completed_at else None,
            "statistics": {
                "total_requested": batch.total_requested,
                "total_success": batch.total_success,
                "total_failed": batch.total_failed,
                "success_rate": batch.success_rate,
            },
            "records": [
                {
                    "metadata": record.metadata.model_dump(),
                    "raw_data": record.raw_data,
                }
                for record in batch.records
            ],
        }

        # Write to file
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(batch_data, f, indent=2, default=str)

        self._logger.info(
            "Wrote batch to storage",
            batch_id=str(batch.batch_id),
            output_path=str(output_path),
            records=len(batch.records),
            success_rate=batch.success_rate,
        )

        return output_path

    def get_table_config(self, source: DataSource) -> BronzeTableConfig:
        """Get table configuration for a data source."""
        return BRONZE_TABLES[source]
