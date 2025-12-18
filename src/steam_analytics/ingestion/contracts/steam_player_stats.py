"""
Data contracts for Steam Player Stats API responses.
"""

from pydantic import BaseModel, Field


class PlayerCountResponse(BaseModel):
    """
    Response from GetNumberOfCurrentPlayers endpoint.

    Endpoint: ISteamUserStats/GetNumberOfCurrentPlayers/v1/
    """

    player_count: int = Field(..., ge=0, description="Current number of players")
    result: int = Field(..., description="Result code (1 = success)")

    @property
    def is_successful(self) -> bool:
        """Check if request was successful."""
        return self.result == 1


class PlayerCountAPIResponse(BaseModel):
    """Wrapper for player count API response."""

    response: PlayerCountResponse
