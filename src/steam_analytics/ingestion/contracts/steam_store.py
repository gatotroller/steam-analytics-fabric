"""
Data contracts for Steam Store API responses.

These Pydantic models define the expected structure of data
from the Steam Store API, providing validation and type safety.
"""

from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, Field, field_validator


class PriceOverview(BaseModel):
    """Price information for a game."""

    currency: str = Field(..., description="Currency code (e.g., USD, EUR)")
    initial: int = Field(..., description="Initial price in cents")
    final: int = Field(..., description="Final price in cents (after discount)")
    discount_percent: int = Field(..., ge=0, le=100, description="Discount percentage")
    initial_formatted: str = Field(default="", description="Formatted initial price")
    final_formatted: str = Field(default="", description="Formatted final price")

    @property
    def initial_dollars(self) -> Decimal:
        """Convert initial price from cents to dollars."""
        return Decimal(self.initial) / 100

    @property
    def final_dollars(self) -> Decimal:
        """Convert final price from cents to dollars."""
        return Decimal(self.final) / 100


class ReleaseDate(BaseModel):
    """Release date information."""

    coming_soon: bool = Field(default=False, description="Whether the game is not yet released")
    date: str = Field(default="", description="Release date string")


class Platform(BaseModel):
    """Platform availability."""

    windows: bool = Field(default=False)
    mac: bool = Field(default=False)
    linux: bool = Field(default=False)


class Category(BaseModel):
    """Game category."""

    id: int
    description: str


class Genre(BaseModel):
    """Game genre."""

    id: str
    description: str


class Screenshot(BaseModel):
    """Game screenshot."""

    id: int
    path_thumbnail: str
    path_full: str


class Metacritic(BaseModel):
    """Metacritic score information."""

    score: int = Field(..., ge=0, le=100)
    url: str = Field(default="")


class SteamStoreGame(BaseModel):
    """
    Complete game data from Steam Store API.

    Represents the full response from /appdetails endpoint.
    """

    # Identifiers
    steam_appid: int = Field(..., description="Steam application ID")
    name: str = Field(..., description="Game name")
    type: str = Field(..., description="Type: game, dlc, demo, etc.")

    # Description
    short_description: str = Field(default="", description="Brief description")
    detailed_description: str = Field(default="", description="Full description")
    about_the_game: str = Field(default="", description="About section")

    # Classification
    is_free: bool = Field(default=False, description="Whether the game is free")
    required_age: int | str = Field(default=0, description="Required age")
    developers: list[str] = Field(default_factory=list)
    publishers: list[str] = Field(default_factory=list)
    categories: list[Category] = Field(default_factory=list)
    genres: list[Genre] = Field(default_factory=list)

    # Pricing (optional - free games don't have this)
    price_overview: PriceOverview | None = Field(
        default=None, description="Price info (None for free games)"
    )

    # Platforms
    platforms: Platform = Field(default_factory=Platform)

    # Release
    release_date: ReleaseDate = Field(default_factory=ReleaseDate)

    # Media
    header_image: str = Field(default="", description="Header image URL")
    screenshots: list[Screenshot] = Field(default_factory=list)

    # Reviews
    metacritic: Metacritic | None = Field(default=None)

    # Additional
    supported_languages: str = Field(default="", description="Supported languages HTML")
    website: str | None = Field(default=None, description="Official website")

    @field_validator("required_age", mode="before")
    @classmethod
    def coerce_required_age(cls, v: int | str) -> int:
        """Convert required_age to int (API sometimes returns string)."""
        if isinstance(v, str):
            return int(v) if v.isdigit() else 0
        return v

    @property
    def genre_names(self) -> list[str]:
        """Extract genre names as simple list."""
        return [g.description for g in self.genres]

    @property
    def category_names(self) -> list[str]:
        """Extract category names as simple list."""
        return [c.description for c in self.categories]


class SteamStoreAPIResponse(BaseModel):
    """
    Wrapper for Steam Store API response.

    The API returns {app_id: {success: bool, data: {...}}}
    """

    success: bool
    data: SteamStoreGame | None = None


# Type alias for batch responses
AppId = Annotated[int, Field(gt=0, description="Steam App ID")]
