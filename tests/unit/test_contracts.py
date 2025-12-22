"""Tests for data contracts."""

import pytest

from src.steam_analytics.ingestion.contracts import (
    PlayerCountResponse,
    PriceOverview,
    ReviewQuerySummary,
    SteamReviewsResponse,
    SteamStoreGame,
)


class TestPriceOverview:
    """Tests for PriceOverview contract."""

    def test_price_conversion(self) -> None:
        """Test cents to dollars conversion."""
        price = PriceOverview(
            currency="USD",
            initial=5999,
            final=2999,
            discount_percent=50,
        )

        assert price.initial_dollars == pytest.approx(59.99)
        assert price.final_dollars == pytest.approx(29.99)

    def test_discount_validation(self) -> None:
        """Test discount percentage bounds."""
        # Valid discount
        price = PriceOverview(
            currency="USD",
            initial=1000,
            final=500,
            discount_percent=50,
        )
        assert price.discount_percent == 50

        # Invalid discount
        with pytest.raises(ValueError):
            PriceOverview(
                currency="USD",
                initial=1000,
                final=500,
                discount_percent=150,
            )


class TestSteamStoreGame:
    """Tests for SteamStoreGame contract."""

    def test_minimal_valid_game(self) -> None:
        """Test creation with minimal required fields."""
        game = SteamStoreGame(
            steam_appid=123456,
            name="Test Game",
            type="game",
        )

        assert game.steam_appid == 123456
        assert game.name == "Test Game"
        assert game.is_free is False
        assert game.genres == []

    def test_required_age_coercion(self) -> None:
        """Test that required_age string is converted to int."""
        game = SteamStoreGame(
            steam_appid=123456,
            name="Test Game",
            type="game",
            required_age="18",
        )

        assert game.required_age == 18
        assert isinstance(game.required_age, int)

    def test_genre_names_property(self) -> None:
        """Test genre_names helper property."""
        game = SteamStoreGame(
            steam_appid=123456,
            name="Test Game",
            type="game",
            genres=[
                {"id": "1", "description": "Action"},
                {"id": "2", "description": "RPG"},
            ],
        )

        assert game.genre_names == ["Action", "RPG"]


class TestSteamReviewsResponse:
    """Tests for SteamReviewsResponse contract."""

    def test_positive_ratio_calculation(self) -> None:
        """Test positive review ratio calculation."""
        response = SteamReviewsResponse(
            success=1,
            query_summary=ReviewQuerySummary(
                total_positive=80,
                total_negative=20,
                total_reviews=100,
            ),
        )

        assert response.positive_ratio == pytest.approx(0.8)

    def test_positive_ratio_zero_reviews(self) -> None:
        """Test positive ratio when no reviews."""
        response = SteamReviewsResponse(
            success=1,
            query_summary=ReviewQuerySummary(total_reviews=0),
        )

        assert response.positive_ratio is None

    def test_is_successful_property(self) -> None:
        """Test is_successful helper property."""
        success_response = SteamReviewsResponse(success=1)
        assert success_response.is_successful is True

        failed_response = SteamReviewsResponse(success=0)
        assert failed_response.is_successful is False


class TestPlayerCountResponse:
    """Tests for PlayerCountResponse contract."""

    def test_valid_response(self) -> None:
        """Test valid player count response."""
        response = PlayerCountResponse(
            player_count=45000,
            result=1,
        )

        assert response.player_count == 45000
        assert response.is_successful is True

    def test_player_count_non_negative(self) -> None:
        """Test that player_count must be non-negative."""
        with pytest.raises(ValueError):
            PlayerCountResponse(
                player_count=-100,
                result=1,
            )
