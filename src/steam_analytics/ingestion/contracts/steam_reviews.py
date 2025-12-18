"""
Data contracts for Steam Reviews API responses.
"""

from pydantic import BaseModel, Field


class ReviewQuerySummary(BaseModel):
    """Summary of review query results."""

    num_reviews: int = Field(default=0, description="Number of reviews returned")
    review_score: int = Field(default=0, ge=0, le=9, description="Review score (0-9 scale)")
    review_score_desc: str = Field(
        default="", description="Review score description (e.g., 'Very Positive')"
    )
    total_positive: int = Field(default=0, ge=0, description="Total positive reviews")
    total_negative: int = Field(default=0, ge=0, description="Total negative reviews")
    total_reviews: int = Field(default=0, ge=0, description="Total review count")


class ReviewAuthor(BaseModel):
    """Review author information."""

    steamid: str = Field(..., description="Steam ID of reviewer")
    num_games_owned: int = Field(default=0, ge=0)
    num_reviews: int = Field(default=0, ge=0)
    playtime_forever: int = Field(default=0, ge=0, description="Total playtime in minutes")
    playtime_last_two_weeks: int = Field(
        default=0, ge=0, description="Playtime in last 2 weeks (minutes)"
    )
    playtime_at_review: int = Field(
        default=0, ge=0, description="Playtime at time of review (minutes)"
    )
    last_played: int = Field(default=0, description="Unix timestamp of last played")


class Review(BaseModel):
    """Individual review data."""

    recommendationid: str = Field(..., description="Unique review ID")
    author: ReviewAuthor
    language: str = Field(default="english")
    review: str = Field(default="", description="Review text content")
    timestamp_created: int = Field(..., description="Unix timestamp of creation")
    timestamp_updated: int = Field(..., description="Unix timestamp of last update")
    voted_up: bool = Field(..., description="True if positive review")
    votes_up: int = Field(default=0, ge=0, description="Helpful votes")
    votes_funny: int = Field(default=0, ge=0, description="Funny votes")
    weighted_vote_score: str = Field(default="0", description="Weighted vote score")
    comment_count: int = Field(default=0, ge=0)
    steam_purchase: bool = Field(default=False, description="Purchased on Steam")
    received_for_free: bool = Field(default=False)
    written_during_early_access: bool = Field(default=False)

    @property
    def playtime_hours(self) -> float:
        """Convert playtime at review from minutes to hours."""
        return self.author.playtime_at_review / 60


class SteamReviewsResponse(BaseModel):
    """
    Complete response from Steam Reviews API.

    Endpoint: /appreviews/{appid}
    """

    success: int = Field(..., description="1 if successful, 0 otherwise")
    query_summary: ReviewQuerySummary = Field(default_factory=ReviewQuerySummary)
    reviews: list[Review] = Field(default_factory=list)
    cursor: str = Field(default="*", description="Cursor for pagination")

    @property
    def is_successful(self) -> bool:
        """Check if API request was successful."""
        return self.success == 1

    @property
    def positive_ratio(self) -> float | None:
        """Calculate positive review ratio."""
        total = self.query_summary.total_reviews
        if total == 0:
            return None
        return self.query_summary.total_positive / total
