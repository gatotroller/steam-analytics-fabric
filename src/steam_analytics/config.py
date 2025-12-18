"""
Application configuration using Pydantic Settings.

Loads configuration from environment variables with validation,
type coercion, and sensible defaults.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
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
    """Microsoft Fabric specific configuration."""

    model_config = SettingsConfigDict(env_prefix="FABRIC_")

    workspace_id: str = Field(
        default=...,
        description="Fabric workspace GUID",
    )
    lakehouse_id: str = Field(
        default=...,
        description="Fabric lakehouse GUID",
    )
    lakehouse_name: str = Field(
        default="steam_analytics",
        description="Lakehouse name for path construction",
    )

    @field_validator("workspace_id", "lakehouse_id")
    @classmethod
    def validate_guid_format(cls, v: str) -> str:
        """Validate that IDs look like GUIDs."""
        import re

        guid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            re.IGNORECASE,
        )
        if not guid_pattern.match(v):
            raise ValueError(f"Invalid GUID format: {v}")
        return v.lower()


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
        extra="ignore",
    )

    # Environment
    environment: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Deployment environment",
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
