"""Tests for Bronze layer schemas."""

from uuid import uuid4

import pytest

from src.steam_analytics.ingestion.bronze.schemas import (
    BRONZE_TABLES,
    BronzeBatch,
    BronzeMetadata,
    BronzeRecord,
    DataSource,
    IngestionStatus,
)


class TestBronzeMetadata:
    """Tests for BronzeMetadata."""

    def test_auto_generated_fields(self) -> None:
        """Test that auto-generated fields are populated."""
        metadata = BronzeMetadata(
            batch_id=uuid4(),
            source=DataSource.STEAM_STORE,
            source_endpoint="https://api.example.com",
            status=IngestionStatus.SUCCESS,
        )

        assert metadata.record_id is not None
        assert metadata.ingested_at is not None
        assert metadata.environment == "development"

    def test_error_fields(self) -> None:
        """Test metadata with error information."""
        metadata = BronzeMetadata(
            batch_id=uuid4(),
            source=DataSource.STEAM_STORE,
            source_endpoint="https://api.example.com",
            status=IngestionStatus.FAILED,
            error_message="Connection timeout",
        )

        assert metadata.status == IngestionStatus.FAILED
        assert metadata.error_message == "Connection timeout"


class TestBronzeBatch:
    """Tests for BronzeBatch."""

    def test_empty_batch(self) -> None:
        """Test empty batch creation."""
        batch = BronzeBatch(
            source=DataSource.STEAM_STORE,
            total_requested=10,
        )

        assert batch.total_requested == 10
        assert batch.total_success == 0
        assert batch.total_failed == 0
        assert batch.success_rate is None  # No records yet
        assert len(batch.records) == 0

    def test_add_success_record(self) -> None:
        """Test adding successful record."""
        batch = BronzeBatch(source=DataSource.STEAM_STORE, total_requested=1)

        record = BronzeRecord(
            metadata=BronzeMetadata(
                batch_id=batch.batch_id,
                source=DataSource.STEAM_STORE,
                source_endpoint="https://api.example.com",
                status=IngestionStatus.SUCCESS,
            ),
            raw_data={"test": "data"},
        )

        batch.add_record(record)

        assert batch.total_success == 1
        assert batch.total_failed == 0
        assert len(batch.records) == 1

    def test_add_failed_record(self) -> None:
        """Test adding failed record."""
        batch = BronzeBatch(source=DataSource.STEAM_STORE, total_requested=1)

        record = BronzeRecord(
            metadata=BronzeMetadata(
                batch_id=batch.batch_id,
                source=DataSource.STEAM_STORE,
                source_endpoint="https://api.example.com",
                status=IngestionStatus.FAILED,
            ),
            raw_data={"error": "failed"},
        )

        batch.add_record(record)

        assert batch.total_success == 0
        assert batch.total_failed == 1

    def test_success_rate_calculation(self) -> None:
        """Test success rate calculation."""
        batch = BronzeBatch(source=DataSource.STEAM_STORE, total_requested=4)

        # Add 3 success, 1 failure
        for status in [
            IngestionStatus.SUCCESS,
            IngestionStatus.SUCCESS,
            IngestionStatus.SUCCESS,
            IngestionStatus.FAILED,
        ]:
            record = BronzeRecord(
                metadata=BronzeMetadata(
                    batch_id=batch.batch_id,
                    source=DataSource.STEAM_STORE,
                    source_endpoint="https://api.example.com",
                    status=status,
                ),
                raw_data={},
            )
            batch.add_record(record)

        assert batch.success_rate == pytest.approx(75.0)

    def test_complete_batch(self) -> None:
        """Test batch completion."""
        batch = BronzeBatch(source=DataSource.STEAM_STORE)

        assert batch.completed_at is None

        batch.complete()

        assert batch.completed_at is not None


class TestBronzeTables:
    """Tests for pre-defined table configurations."""

    def test_all_sources_have_config(self) -> None:
        """Test that all data sources have table configs."""
        for source in DataSource:
            assert source in BRONZE_TABLES

    def test_table_paths(self) -> None:
        """Test table path generation."""
        config = BRONZE_TABLES[DataSource.STEAM_STORE]

        assert config.table_name == "raw_steam_store"
        assert config.full_path == "Tables/bronze/raw_steam_store"
