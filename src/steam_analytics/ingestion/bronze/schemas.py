"""
Bronze layer schemas for raw data storage.

These schemas define the structure of raw data as it lands
in the Bronze layer, including metadata for tracking and lineage.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class DataSource(str, Enum):
    """Enumeration of data sources."""

    STEAM_STORE = "steam_store_api"
    STEAM_REVIEWS = "steam_reviews_api"
    STEAM_PLAYER_STATS = "steam_player_stats_api"


class IngestionStatus(str, Enum):
    """Status of an ingestion record."""

    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class BronzeMetadata(BaseModel):
    """
    Metadata attached to every Bronze layer record.

    This metadata enables:
    - Lineage tracking (where did this data come from?)
    - Auditing (when was it ingested? by what process?)
    - Reprocessing (which batch does it belong to?)
    - Debugging (what was the original request?)
    """

    # Identifiers
    record_id: UUID = Field(default_factory=uuid4, description="Unique record identifier")
    batch_id: UUID = Field(..., description="Batch/run identifier for grouping")

    # Source tracking
    source: DataSource = Field(..., description="Data source identifier")
    source_endpoint: str = Field(..., description="Full API endpoint called")

    # Timing
    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of ingestion",
    )
    api_response_time_ms: float | None = Field(
        default=None, description="API response time in milliseconds"
    )

    # Status
    status: IngestionStatus = Field(..., description="Ingestion status")
    error_message: str | None = Field(default=None, description="Error message if failed")

    # Request context
    request_params: dict[str, Any] = Field(
        default_factory=dict, description="Parameters sent to API"
    )

    # Environment
    environment: str = Field(default="development", description="Deployment environment")
    pipeline_version: str = Field(default="0.1.0", description="Pipeline code version")


class BronzeRecord(BaseModel):
    """
    A single record in the Bronze layer.

    Combines raw data with metadata for complete traceability.
    """

    metadata: BronzeMetadata
    raw_data: dict[str, Any] = Field(..., description="Raw API response as-is")

    class Config:
        json_schema_extra: ClassVar[dict[str, Any]] = {
            "example": {
                "metadata": {
                    "record_id": "550e8400-e29b-41d4-a716-446655440000",
                    "batch_id": "660e8400-e29b-41d4-a716-446655440000",
                    "source": "steam_store_api",
                    "source_endpoint": "https://store.steampowered.com/api/appdetails?appids=1091500",
                    "ingested_at": "2024-01-15T10:30:00Z",
                    "api_response_time_ms": 150.5,
                    "status": "success",
                    "request_params": {"appids": 1091500, "cc": "US"},
                    "environment": "production",
                    "pipeline_version": "0.1.0",
                },
                "raw_data": {"1091500": {"success": True, "data": {}}},
            }
        }


class BronzeBatch(BaseModel):
    """
    A batch of Bronze records from a single ingestion run.

    Used for writing multiple records together and tracking
    batch-level statistics.
    """

    batch_id: UUID = Field(default_factory=uuid4)
    source: DataSource
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    records: list[BronzeRecord] = Field(default_factory=list)

    # Statistics
    total_requested: int = 0
    total_success: int = 0
    total_failed: int = 0

    @property
    def success_rate(self) -> float | None:
        """Calculate success rate as percentage."""
        if self.total_requested == 0:
            return None
        return (self.total_success / self.total_requested) * 100

    def add_record(self, record: BronzeRecord) -> None:
        """Add a record and update statistics."""
        self.records.append(record)
        if record.metadata.status == IngestionStatus.SUCCESS:
            self.total_success += 1
        else:
            self.total_failed += 1

    def complete(self) -> None:
        """Mark batch as complete."""
        self.completed_at = datetime.now(timezone.utc)


class BronzeTableConfig(BaseModel):
    """
    Configuration for a Bronze layer table.

    Defines partitioning, path structure, and retention policies.
    """

    table_name: str = Field(..., description="Table name in lakehouse")
    source: DataSource
    partition_columns: list[str] = Field(
        default=["_ingestion_date"],
        description="Columns to partition by",
    )
    base_path: str = Field(
        default="Tables/bronze",
        description="Base path in lakehouse",
    )
    retention_days: int | None = Field(
        default=None,
        description="Days to retain data (None = forever)",
    )

    @property
    def full_path(self) -> str:
        """Get full table path."""
        return f"{self.base_path}/{self.table_name}"


# Pre-defined table configurations
BRONZE_TABLES = {
    DataSource.STEAM_STORE: BronzeTableConfig(
        table_name="raw_steam_store",
        source=DataSource.STEAM_STORE,
    ),
    DataSource.STEAM_REVIEWS: BronzeTableConfig(
        table_name="raw_steam_reviews",
        source=DataSource.STEAM_REVIEWS,
    ),
    DataSource.STEAM_PLAYER_STATS: BronzeTableConfig(
        table_name="raw_steam_player_stats",
        source=DataSource.STEAM_PLAYER_STATS,
    ),
}
