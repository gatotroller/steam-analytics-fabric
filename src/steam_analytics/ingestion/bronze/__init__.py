"""
Bronze layer for raw data storage.

Supports multiple output targets:
- Local filesystem (BronzeWriter)
- OneLake/Fabric (OneLakeWriter)
"""

from src.steam_analytics.ingestion.bronze.schemas import (
    BRONZE_TABLES,
    BronzeBatch,
    BronzeMetadata,
    BronzeRecord,
    BronzeTableConfig,
    DataSource,
    IngestionStatus,
)
from src.steam_analytics.ingestion.bronze.writer import BronzeWriter

__all__ = [
    "BRONZE_TABLES",
    "BronzeBatch",
    "BronzeMetadata",
    "BronzeRecord",
    "BronzeTableConfig",
    "BronzeWriter",
    "DataSource",
    "IngestionStatus",
]

# Conditionally export OneLakeWriter (requires azure packages)
try:
    from src.steam_analytics.ingestion.bronze.onelake_writer import OneLakeWriter  # noqa: F401

    __all__.append("OneLakeWriter")
except ImportError:
    pass
