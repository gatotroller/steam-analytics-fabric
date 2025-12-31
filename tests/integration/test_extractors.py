"""Integration tests for Steam extractors with mocked HTTP responses."""

import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import httpx
import pytest
import respx
from steam_analytics.ingestion.extractors import (
    SteamPlayerStatsExtractor,
    SteamReviewsExtractor,
    SteamStoreExtractor,
)

# Load fixtures
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def load_fixture(name: str) -> dict[str, Any]:
    """Load a JSON fixture file."""
    # Usamos .open() como pidió Ruff
    with (FIXTURES_DIR / name).open(encoding="utf-8") as f:
        # Usamos cast para jurarle a Mypy que el JSON es un dict
        return cast(dict[str, Any], json.load(f))


@pytest.fixture
def mock_env() -> Any:
    """Mock environment variables for tests."""
    with patch.dict(
        "os.environ",
        {
            "STEAM_API_KEY": "test_api_key_123",
            "FABRIC_WORKSPACE_ID": "12345678-1234-1234-1234-123456789abc",
            "FABRIC_LAKEHOUSE_ID": "87654321-4321-4321-4321-cba987654321",
        },
    ):
        yield


@pytest.fixture
def store_response() -> dict[str, Any]:
    """Load Steam Store API response fixture."""
    return load_fixture("steam_store_response.json")


@pytest.fixture
def reviews_response() -> dict[str, Any]:
    """Load Steam Reviews API response fixture."""
    return load_fixture("steam_reviews_response.json")


@pytest.fixture
def player_count_response() -> dict[str, Any]:
    """Load Steam Player Count API response fixture."""
    return load_fixture("steam_player_count_response.json")


class TestSteamStoreExtractor:
    """Integration tests for Steam Store extractor."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_extract_success(
        self,
        mock_env: None,
        store_response: dict[str, Any],
    ) -> None:
        """Test successful game extraction."""
        # Setup mock
        respx.get("https://store.steampowered.com/api/appdetails").mock(
            return_value=httpx.Response(200, json=store_response)
        )

        # Extract
        async with SteamStoreExtractor() as extractor:
            result = await extractor.extract(app_id=1091500)

        # Verify
        assert result.success is True
        assert result.data is not None
        assert result.data.name == "Cyberpunk 2077"
        assert result.data.steam_appid == 1091500
        assert result.data.price_overview is not None
        # Corrección del float vs decimal que hicimos antes
        assert float(result.data.price_overview.final_dollars) == pytest.approx(29.99)
        assert result.data.genre_names == ["Action", "RPG"]
        assert result.source == "steam_store_api"
        assert result.duration_ms is not None
        assert result.duration_ms > 0

    @respx.mock
    @pytest.mark.asyncio
    async def test_extract_not_found(self, mock_env: None) -> None:
        """Test extraction when game doesn't exist."""
        # Setup mock - Steam returns success=false for non-existent games
        respx.get("https://store.steampowered.com/api/appdetails").mock(
            return_value=httpx.Response(
                200,
                json={"999999999": {"success": False}},
            )
        )

        # Extract
        async with SteamStoreExtractor() as extractor:
            result = await extractor.extract(app_id=999999999)

        # Verify
        assert result.success is False
        assert result.error_message is not None
        assert "success=false" in result.error_message

    @respx.mock
    @pytest.mark.asyncio
    async def test_extract_api_error(self, mock_env: None) -> None:
        """Test extraction handles API errors."""
        # Setup mock - Return 500 error
        respx.get("https://store.steampowered.com/api/appdetails").mock(
            return_value=httpx.Response(500)
        )

        # Extract
        async with SteamStoreExtractor() as extractor:
            result = await extractor.extract(app_id=1091500)

        # Verify
        assert result.success is False
        assert result.error_message is not None

    @respx.mock
    @pytest.mark.asyncio
    async def test_extract_batch(
        self,
        mock_env: None,
        store_response: dict[str, Any],
    ) -> None:
        """Test batch extraction."""
        # Setup mock
        respx.get("https://store.steampowered.com/api/appdetails").mock(
            return_value=httpx.Response(200, json=store_response)
        )

        # Extract batch
        async with SteamStoreExtractor() as extractor:
            results = await extractor.extract_batch(
                app_ids=[1091500, 1091500],  # Same ID twice for simplicity
            )

        # Verify
        assert len(results) == 2
        assert all(r.success for r in results)


class TestSteamReviewsExtractor:
    """Integration tests for Steam Reviews extractor."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_extract_success(
        self,
        mock_env: None,
        reviews_response: dict[str, Any],
    ) -> None:
        """Test successful reviews extraction."""
        # Setup mock
        respx.get("https://store.steampowered.com/appreviews/1091500").mock(
            return_value=httpx.Response(200, json=reviews_response)
        )

        # Extract
        async with SteamReviewsExtractor() as extractor:
            result = await extractor.extract(app_id=1091500)

        # Verify
        assert result.success is True
        assert result.data is not None
        assert result.data.query_summary.total_reviews == 570000
        assert result.data.positive_ratio == pytest.approx(0.789, rel=0.01)
        assert len(result.data.reviews) == 1
        assert result.source == "steam_reviews_api"

    @respx.mock
    @pytest.mark.asyncio
    async def test_extract_summary_only(
        self,
        mock_env: None,
        reviews_response: dict[str, Any],
    ) -> None:
        """Test summary-only extraction."""
        # Setup mock
        respx.get("https://store.steampowered.com/appreviews/1091500").mock(
            return_value=httpx.Response(200, json=reviews_response)
        )

        # Extract summary only
        async with SteamReviewsExtractor() as extractor:
            result = await extractor.extract_summary_only(app_id=1091500)

        # Verify
        assert result.success is True
        assert result.data is not None


class TestSteamPlayerStatsExtractor:
    """Integration tests for Steam Player Stats extractor."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_extract_success(
        self,
        mock_env: None,
        player_count_response: dict[str, Any],
    ) -> None:
        """Test successful player count extraction."""
        # Setup mock
        respx.get(
            "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
        ).mock(return_value=httpx.Response(200, json=player_count_response))

        # Extract
        async with SteamPlayerStatsExtractor() as extractor:
            result = await extractor.extract(app_id=1091500)

        # Verify
        assert result.success is True
        assert result.data is not None
        assert result.data.player_count == 45000
        assert result.data.is_successful is True
        assert result.source == "steam_player_stats_api"

    @respx.mock
    @pytest.mark.asyncio
    async def test_extract_batch(
        self,
        mock_env: None,
        player_count_response: dict[str, Any],
    ) -> None:
        """Test batch player count extraction."""
        # Setup mock
        respx.get(
            "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
        ).mock(return_value=httpx.Response(200, json=player_count_response))

        # Extract batch
        async with SteamPlayerStatsExtractor() as extractor:
            results = await extractor.extract_batch(
                app_ids=[1091500, 570],
            )

        # Verify
        assert len(results) == 2
        assert all(r.success for r in results)
