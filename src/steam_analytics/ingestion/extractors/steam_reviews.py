"""
Steam Reviews API extractor.

Fetches user reviews and review summaries from Steam's Reviews API.
"""

import time
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from steam_analytics.config import get_settings
from steam_analytics.ingestion.contracts import AppId, SteamReviewsResponse
from steam_analytics.ingestion.extractors.base import (
    BaseExtractor,
    ExtractionError,
    ExtractionResult,
    ValidationError,
)
from steam_analytics.ingestion.utils.rate_limiter import RateLimiter, RateLimiterConfig


class SteamReviewsExtractor(BaseExtractor[SteamReviewsResponse]):
    """
    Extractor for Steam Reviews API.

    Fetches user reviews and summary statistics for games.
    Supports pagination through cursor parameter.

    Example:
        >>> async with SteamReviewsExtractor() as extractor:
        ...     result = await extractor.extract(app_id=1091500)
        ...     if result.success:
        ...         print(f"Positive ratio: {result.data.positive_ratio}")
    """

    def __init__(
        self,
        *,
        rate_limiter: RateLimiter | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize Steam Reviews extractor.

        Args:
            rate_limiter: Custom rate limiter (creates default if None)
            **kwargs: Arguments passed to BaseExtractor
        """
        super().__init__(**kwargs)
        settings = get_settings()
        self._store_url = settings.steam.store_url.replace("/api", "")
        self._rate_limiter = rate_limiter or RateLimiter(
            RateLimiterConfig(
                requests_per_minute=settings.steam.requests_per_minute,
            )
        )

    @property
    def source_name(self) -> str:
        """Return source identifier."""
        return "steam_reviews_api"

    def _build_url(self, app_id: AppId) -> str:
        """Build API URL for app reviews."""
        return f"{self._store_url}/appreviews/{app_id}"

    def _parse_response(self, raw_data: dict[str, Any]) -> SteamReviewsResponse:
        """
        Parse and validate Steam Reviews API response.

        Args:
            raw_data: Raw JSON response from API

        Returns:
            SteamReviewsResponse: Validated reviews data

        Raises:
            ValidationError: If response doesn't match expected schema
        """
        try:
            return SteamReviewsResponse.model_validate(raw_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Response validation failed: {e}",
                source=self.source_name,
            ) from e

    async def extract(
        self,
        app_id: AppId,
        *,
        filter_type: str = "recent",
        language: str = "all",
        review_type: str = "all",
        purchase_type: str = "all",
        num_per_page: int = 20,
        cursor: str = "*",
    ) -> ExtractionResult[SteamReviewsResponse]:
        """
        Extract reviews from Steam Reviews API.

        Args:
            app_id: Steam application ID
            filter_type: Filter by 'recent' or 'updated'
            language: Language filter ('all' or specific language)
            review_type: 'all', 'positive', or 'negative'
            purchase_type: 'all', 'steam', or 'non_steam_purchase'
            num_per_page: Number of reviews per page (max 100)
            cursor: Pagination cursor ('*' for first page)

        Returns:
            ExtractionResult[SteamReviewsResponse]: Extraction result with metadata
        """
        url = self._build_url(app_id)
        start_time = time.perf_counter()

        self._logger.info(
            "Starting reviews extraction",
            app_id=app_id,
            filter_type=filter_type,
        )

        try:
            # Respect rate limits
            await self._rate_limiter.acquire()

            # Make request
            response = await self._make_request(
                "GET",
                url,
                params={
                    "json": 1,
                    "filter": filter_type,
                    "language": language,
                    "review_type": review_type,
                    "purchase_type": purchase_type,
                    "num_per_page": num_per_page,
                    "cursor": cursor,
                },
            )

            raw_data = response.json()
            duration_ms = (time.perf_counter() - start_time) * 1000

            # Parse and validate
            reviews_data = self._parse_response(raw_data)

            if not reviews_data.is_successful:
                self._logger.warning(
                    "API returned unsuccessful response",
                    app_id=app_id,
                )
                return ExtractionResult(
                    success=False,
                    error_message=f"Steam Reviews API returned success=0 for app_id={app_id}",
                    source=self.source_name,
                    endpoint=url,
                    duration_ms=duration_ms,
                    raw_response=raw_data,
                )

            self._logger.info(
                "Reviews extraction successful",
                app_id=app_id,
                total_reviews=reviews_data.query_summary.total_reviews,
                reviews_fetched=len(reviews_data.reviews),
                positive_ratio=reviews_data.positive_ratio,
                duration_ms=round(duration_ms, 2),
            )

            return ExtractionResult(
                success=True,
                data=reviews_data,
                source=self.source_name,
                endpoint=url,
                duration_ms=duration_ms,
                extracted_at=datetime.now(timezone.utc),
            )

        except ValidationError as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            self._logger.error(
                "Validation failed",
                app_id=app_id,
                error=str(e),
            )
            return ExtractionResult(
                success=False,
                error_message=str(e),
                source=self.source_name,
                endpoint=url,
                duration_ms=duration_ms,
            )

        except ExtractionError as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            self._logger.error(
                "Extraction failed",
                app_id=app_id,
                error=str(e),
                status_code=e.status_code,
            )
            return ExtractionResult(
                success=False,
                error_message=str(e),
                source=self.source_name,
                endpoint=url,
                duration_ms=duration_ms,
            )

    async def extract_summary_only(
        self,
        app_id: AppId,
    ) -> ExtractionResult[SteamReviewsResponse]:
        """
        Extract only review summary (no individual reviews).

        Faster extraction when you only need aggregate stats.

        Args:
            app_id: Steam application ID

        Returns:
            ExtractionResult[SteamReviewsResponse]: Result with summary only
        """
        return await self.extract(
            app_id,
            num_per_page=0,  # Don't fetch individual reviews
        )
