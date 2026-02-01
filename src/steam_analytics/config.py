"""
Application configuration using Pydantic Settings.

Loads configuration from environment variables with validation,
type coercion, and sensible defaults.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SteamAPIConfig(BaseSettings):
    """Steam API specific configuration."""

    model_config = SettingsConfigDict(env_prefix="STEAM_")

    api_key: SecretStr = Field(
        default=...,
        description="Steam Web API key from https://steamcommunity.com/dev/apikey",
    )
    base_url: str = Field(
        default="https://api.steampowered.com",
        description="Base URL for Steam Web API",
    )
    store_url: str = Field(
        default="https://store.steampowered.com/api",
        description="Base URL for Steam Store API",
    )
    requests_per_minute: int = Field(
        default=40,
        ge=1,
        le=200,
        description="Rate limit for API requests per minute",
    )
    timeout_seconds: int = Field(
        default=30,
        ge=5,
        le=120,
        description="HTTP request timeout in seconds",
    )


class FabricConfig(BaseSettings):
    """Microsoft Fabric configuration with multi-lakehouse support."""

    model_config = SettingsConfigDict(env_prefix="FABRIC_")

    # Workspace
    workspace_id: str = Field(default=..., description="Fabric workspace GUID")

    # Lakehouse IDs
    bronze_lakehouse_id: str = Field(default=..., description="Bronze lakehouse GUID")
    silver_lakehouse_id: str = Field(default=..., description="Silver lakehouse GUID")
    gold_lakehouse_id: str = Field(default=..., description="Gold lakehouse GUID")

    # Lakehouse names (for display)
    bronze_lakehouse_name: str = Field(default="lh_bronze")
    silver_lakehouse_name: str = Field(default="lh_silver")
    gold_lakehouse_name: str = Field(default="lh_gold")

    @field_validator(
        "workspace_id",
        "bronze_lakehouse_id",
        "silver_lakehouse_id",
        "gold_lakehouse_id",
    )
    @classmethod
    def validate_guid_format(cls, v: str) -> str:
        """Validate GUID format."""
        import re

        pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            re.IGNORECASE,
        )
        if not pattern.match(v):
            raise ValueError(f"Invalid GUID format: {v}")
        return v.lower()

    # =========================================================================
    # OneLake Path Helpers
    # =========================================================================

    @computed_field  # type: ignore[misc]
    @property
    def onelake_endpoint(self) -> str:
        """OneLake DFS endpoint."""
        return "https://onelake.dfs.fabric.microsoft.com"

    @computed_field  # type: ignore[misc]
    @property
    def bronze_abfss_path(self) -> str:
        """ABFSS path for Bronze lakehouse."""
        return f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.bronze_lakehouse_id}"

    @computed_field  # type: ignore[misc]
    @property
    def silver_abfss_path(self) -> str:
        """ABFSS path for Silver lakehouse."""
        return f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.silver_lakehouse_id}"

    @computed_field  # type: ignore[misc]
    @property
    def gold_abfss_path(self) -> str:
        """ABFSS path for Gold lakehouse."""
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.gold_lakehouse_id}"
        )

    def get_bronze_table_path(self, table_name: str) -> str:
        """Get full ABFSS path for a Bronze Delta table."""
        return f"{self.bronze_abfss_path}/Tables/{table_name}"

    def get_silver_table_path(self, table_name: str) -> str:
        """Get full ABFSS path for a Silver Delta table."""
        return f"{self.silver_abfss_path}/Tables/{table_name}"

    def get_gold_table_path(self, table_name: str) -> str:
        """Get full ABFSS path for a Gold Delta table."""
        return f"{self.gold_abfss_path}/Tables/{table_name}"


class RetryConfig(BaseSettings):
    """Retry behavior configuration."""

    model_config = SettingsConfigDict(env_prefix="RETRY_")

    max_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of retry attempts",
    )
    base_delay_seconds: float = Field(
        default=1.0,
        ge=0.1,
        le=30.0,
        description="Base delay between retries (exponential backoff)",
    )
    max_delay_seconds: float = Field(
        default=60.0,
        ge=1.0,
        le=300.0,
        description="Maximum delay between retries",
    )
    exponential_base: float = Field(
        default=2.0,
        ge=1.5,
        le=4.0,
        description="Base for exponential backoff calculation",
    )


class LoggingConfig(BaseSettings):
    """Logging configuration."""

    model_config = SettingsConfigDict(env_prefix="LOG_")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level",
    )
    format: Literal["json", "console"] = Field(
        default="json",
        description="Log output format",
    )
    include_timestamp: bool = Field(
        default=True,
        description="Include timestamp in log entries",
    )


class Settings(BaseSettings):
    """
    Main application settings.

    Aggregates all configuration sections and provides
    a single entry point for configuration access.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Environment
    environment: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Deployment environment",
    )

    # Storage mode
    storage_mode: Literal["local", "fabric"] = Field(
        default="local",
        description="Where to store data: local filesystem or Fabric Lakehouse",
    )

    # Sub-configurations
    steam: SteamAPIConfig = Field(default_factory=SteamAPIConfig)
    fabric: FabricConfig = Field(default_factory=FabricConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "production"

    @property
    def is_fabric_mode(self) -> bool:
        """Check if running in Fabric storage mode."""
        return self.storage_mode == "fabric"


@lru_cache
def get_settings() -> Settings:
    """
    Get cached application settings.

    Uses lru_cache to ensure settings are only loaded once
    and reused across the application.

    Returns:
        Settings: Application configuration instance
    """
    return Settings()
