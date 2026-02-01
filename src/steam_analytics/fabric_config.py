"""
Fabric-specific configuration that works without .env files.

In Fabric notebooks, we use widgets or secrets instead of .env files.
This module provides a way to configure the application in Fabric.
"""

from typing import Any

from src.steam_analytics.config import (
    FabricConfig,
    LoggingConfig,
    RetryConfig,
    Settings,
    SteamAPIConfig,
)


def create_fabric_settings(
    *,
    steam_api_key: str,
    workspace_id: str,
    bronze_lakehouse_id: str,
    silver_lakehouse_id: str,
    gold_lakehouse_id: str,
    environment: str = "production",
    **kwargs: Any,
) -> Settings:
    """
    Create Settings object for Fabric environment.

    Use this in notebooks instead of get_settings() which reads from .env.

    Example:
        >>> settings = create_fabric_settings(
        ...     steam_api_key=dbutils.secrets.get("steam", "api-key"),
        ...     workspace_id="...",
        ...     bronze_lakehouse_id="...",
        ...     silver_lakehouse_id="...",
        ...     gold_lakehouse_id="...",
        ... )
    """
    return Settings(
        environment=environment,
        storage_mode="fabric",
        steam=SteamAPIConfig(
            api_key=steam_api_key,
            requests_per_minute=kwargs.get("requests_per_minute", 40),
            timeout_seconds=kwargs.get("timeout_seconds", 30),
        ),
        fabric=FabricConfig(
            workspace_id=workspace_id,
            bronze_lakehouse_id=bronze_lakehouse_id,
            silver_lakehouse_id=silver_lakehouse_id,
            gold_lakehouse_id=gold_lakehouse_id,
            bronze_lakehouse_name=kwargs.get("bronze_lakehouse_name", "lh_bronze"),
            silver_lakehouse_name=kwargs.get("silver_lakehouse_name", "lh_silver"),
            gold_lakehouse_name=kwargs.get("gold_lakehouse_name", "lh_gold"),
        ),
        retry=RetryConfig(
            max_attempts=kwargs.get("retry_max_attempts", 3),
            base_delay_seconds=kwargs.get("retry_base_delay", 1.0),
            max_delay_seconds=kwargs.get("retry_max_delay", 60.0),
        ),
        logging=LoggingConfig(
            level=kwargs.get("log_level", "INFO"),
            format=kwargs.get("log_format", "console"),
        ),
    )
