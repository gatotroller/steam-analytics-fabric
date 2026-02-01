"""
OneLake writer for direct writes from local Python to Fabric.

Enables running extraction locally and writing directly to
Bronze lakehouse in Fabric without needing Spark.
"""

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
from azure.storage.filedatalake import DataLakeServiceClient

from src.steam_analytics.config import get_settings
from src.steam_analytics.ingestion.bronze.schemas import (
    BRONZE_TABLES,
    BronzeBatch,
    BronzeMetadata,
    BronzeRecord,
    DataSource,
    IngestionStatus,
)
from src.steam_analytics.logger import get_logger


class OneLakeWriter:
    """
    Writes extraction results directly to OneLake from local Python.

    Uses Azure Identity for authentication. Works with:
    - Azure CLI login (az login)
    - Interactive browser login
    - Managed identity (when running in Azure)
    """

    ONELAKE_URL = "https://onelake.dfs.fabric.microsoft.com"

    def __init__(self, *, credential: Any = None) -> None:
        """
        Initialize OneLake writer.

        Args:
            credential: Azure credential (auto-detects if None)
        """
        self._settings = get_settings()
        self._workspace_id = self._settings.fabric.workspace_id

        # CORRECTED: Use bronze_lakehouse_id specifically
        self._lakehouse_id = self._settings.fabric.bronze_lakehouse_id

        self._environment = self._settings.environment
        self._logger = get_logger(__name__, component="onelake_writer")

        # Setup Azure credential
        if credential:
            self._credential = credential
        else:
            self._credential = self._get_credential()

        # Create DataLake client
        self._service_client = DataLakeServiceClient(
            account_url=self.ONELAKE_URL,
            credential=self._credential,
        )

        # Filesystem = workspace
        self._filesystem = self._service_client.get_file_system_client(
            file_system=self._workspace_id
        )

        self._logger.info(
            "OneLake writer initialized",
            workspace_id=self._workspace_id,
            lakehouse_id=self._lakehouse_id,
        )

    def _get_credential(self) -> Any:
        """Get Azure credential with fallback."""
        try:
            cred = DefaultAzureCredential()
            # Test the credential (optional, but good practice)
            self._logger.debug("Using DefaultAzureCredential")
            return cred
        except Exception as e:
            self._logger.warning(f"DefaultAzureCredential failed: {e}")
            self._logger.info("Falling back to InteractiveBrowserCredential")
            return InteractiveBrowserCredential()

    def _ensure_directory(self, path: str) -> None:
        """Create directory if it doesn't exist."""
        try:
            directory = self._filesystem.get_directory_client(path)
            directory.create_directory()
        except Exception:
            pass  # Already exists or permission issue handled downstream

    # -------------------------------------------------------------------------
    # GENERIC INTERFACE (Compatible with Orchestrator)
    # -------------------------------------------------------------------------

    def create_batch(self, source: DataSource, total_requested: int = 0) -> BronzeBatch:
        """Start a new ingestion batch."""
        return BronzeBatch(batch_id=uuid4(), source=source, total_requested=total_requested)

    def add_to_batch(
        self, batch: BronzeBatch, result: Any, request_params: dict[str, Any] | None = None
    ) -> None:
        """
        Add a single extraction result to the batch in memory.
        Transforms ExtractionResult to BronzeRecord.
        """
        metadata = BronzeMetadata(
            batch_id=batch.batch_id,
            source=batch.source,
            source_endpoint=result.endpoint,
            ingested_at=datetime.now(timezone.utc),
            api_response_time_ms=result.duration_ms,
            status=IngestionStatus.SUCCESS if result.success else IngestionStatus.FAILED,
            error_message=result.error_message,
            request_params=request_params,
            environment=self._environment,
            pipeline_version="0.1.0",
        )

        # Extract Raw Data
        raw_data = {}
        if result.success and result.data:
            # If Pydantic model, convert to dict
            if hasattr(result.data, "model_dump"):
                raw_data = result.data.model_dump(mode="json")
            elif isinstance(result.data, dict):
                raw_data = result.data
            else:
                raw_data = {"data": str(result.data)}

        # In case of error, store error as raw_data for debug
        if not result.success:
            raw_data = {"error": result.error_message}

        record = BronzeRecord(metadata=metadata, raw_data=raw_data)

        batch.records.append(record)

    def write_batch(self, batch: BronzeBatch) -> str:
        """
        Write the complete batch to OneLake as a single JSON file.

        Returns:
            str: The OneLake path (URL format)
        """
        # 1. Finalize batch
        batch.complete()

        # 2. Get table config
        table_config = BRONZE_TABLES[batch.source]

        # 3. Build paths
        ingestion_date = batch.started_at.strftime("%Y-%m-%d")
        filename = f"batch_{batch.batch_id}_{batch.started_at.strftime('%H%M%S')}.json"

        # Fabric Structure: <LakehouseID>/Files/bronze/<table_name>/date=YYYY-MM-DD/filename
        # NOTE: In OneLake API, path is relative to Filesystem (Workspace)
        directory_path = (
            f"{self._lakehouse_id}/Files/bronze/{table_config.table_name}/date={ingestion_date}"
        )
        full_path_relative = f"{directory_path}/{filename}"

        try:
            # 4. Ensure directory
            self._ensure_directory(directory_path)

            # 5. Serialize to JSON
            content_bytes = batch.model_dump_json(indent=2).encode("utf-8")

            # 6. Upload file
            file_client = self._filesystem.get_file_client(full_path_relative)
            file_client.upload_data(content_bytes, overwrite=True)

            self._logger.info(
                "Uploaded batch to OneLake",
                path=full_path_relative,
                records=len(batch.records),
                size_bytes=len(content_bytes),
            )

            # Return URI format for logs
            return f"abfss://{self._workspace_id}@onelake.dfs.fabric.microsoft.com/{full_path_relative}"

        except Exception as e:
            self._logger.error("Failed to upload batch to OneLake", error=str(e))
            raise e
