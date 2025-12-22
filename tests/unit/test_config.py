"""Tests for configuration module."""

import os
from unittest.mock import patch

import pytest

from src.steam_analytics.config import (
    FabricConfig,
    LoggingConfig,
    RetryConfig,
    SteamAPIConfig,
)


class TestSteamAPIConfig:
    """Tests for Steam API configuration."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        with patch.dict(os.environ, {"STEAM_API_KEY": "test_key"}):
            config = SteamAPIConfig()

        assert config.base_url == "https://api.steampowered.com"
        assert config.store_url == "https://store.steampowered.com/api"
        assert config.requests_per_minute == 40
        assert config.timeout_seconds == 30

    def test_api_key_required(self) -> None:
        """Test that API key is required."""
        with patch.dict(os.environ, {}, clear=True), pytest.raises(ValueError):
            SteamAPIConfig()

    def test_api_key_secret(self) -> None:
        """Test that API key is stored as secret."""
        with patch.dict(os.environ, {"STEAM_API_KEY": "secret_key_123"}):
            config = SteamAPIConfig()

        # SecretStr should not expose value in repr
        assert "secret_key_123" not in repr(config.api_key)
        # But should be accessible via get_secret_value()
        assert config.api_key.get_secret_value() == "secret_key_123"


class TestFabricConfig:
    """Tests for Fabric configuration."""

    def test_valid_guid(self) -> None:
        """Test valid GUID format."""
        with patch.dict(
            os.environ,
            {
                "FABRIC_WORKSPACE_ID": "12345678-1234-1234-1234-123456789abc",
                "FABRIC_LAKEHOUSE_ID": "87654321-4321-4321-4321-cba987654321",
            },
        ):
            config = FabricConfig()

        assert config.workspace_id == "12345678-1234-1234-1234-123456789abc"
        assert config.lakehouse_id == "87654321-4321-4321-4321-cba987654321"

    def test_invalid_guid_format(self) -> None:
        """Test that invalid GUID format raises error."""
        with (
            patch.dict(
                os.environ,
                {
                    "FABRIC_WORKSPACE_ID": "not-a-valid-guid",
                    "FABRIC_LAKEHOUSE_ID": "87654321-4321-4321-4321-cba987654321",
                },
            ),
            pytest.raises(ValueError, match="Invalid GUID format"),
        ):
            FabricConfig()


class TestRetryConfig:
    """Tests for retry configuration."""

    def test_default_values(self) -> None:
        """Test default retry values."""
        config = RetryConfig()

        assert config.max_attempts == 3
        assert config.base_delay_seconds == 1.0
        assert config.max_delay_seconds == 60.0
        assert config.exponential_base == 2.0

    def test_max_attempts_bounds(self) -> None:
        """Test max_attempts validation bounds."""
        with patch.dict(os.environ, {"RETRY_MAX_ATTEMPTS": "0"}), pytest.raises(ValueError):
            RetryConfig()

        with patch.dict(os.environ, {"RETRY_MAX_ATTEMPTS": "11"}), pytest.raises(ValueError):
            RetryConfig()


class TestLoggingConfig:
    """Tests for logging configuration."""

    def test_valid_levels(self) -> None:
        """Test valid log levels."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            with patch.dict(os.environ, {"LOG_LEVEL": level}):
                config = LoggingConfig()
                assert config.level == level

    def test_valid_formats(self) -> None:
        """Test valid log formats."""
        for fmt in ["json", "console"]:
            with patch.dict(os.environ, {"LOG_FORMAT": fmt}):
                config = LoggingConfig()
                assert config.format == fmt
