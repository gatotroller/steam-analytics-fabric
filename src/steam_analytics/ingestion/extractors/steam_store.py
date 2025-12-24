"""
Steam Store API extractor.

Fetches game details from Steam's Store API including
prices, descriptions, tags, and metadata.
"""

import time
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from steam_analytics.config import get_settings
from steam_analytics.ingestion.contracts import (
    AppId,
    SteamStoreGame,
)
from steam_analytics.ingestion.extractors.base import (
    BaseExtractor,
    ExtractionError,
    ExtractionResult,
    ValidationError,
)
from steam_analytics.ingestion.utils.rate_limiter import RateLimiter, RateLimiterConfig


class SteamStoreExtractor(BaseExtractor[SteamStoreGame]):
    """
    Extractor for Steam Store API.

    Fetches detailed game information from the Steam Store API.
    Handles rate limiting, retries, and response validation.

    Example:
        >>> async with SteamStoreExtractor() as extractor:
        ...     result = await extractor.extract(app_id=1091500)
        ...     if result.success:
        ...         print(result.data.name)
    """

    def __init__(
        self,
        *,
        rate_limiter: RateLimiter | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize Steam Store extractor.

        Args:
            rate_limiter: Custom rate limiter (creates default if None)
            **kwargs: Arguments passed to BaseExtractor
        """
        super().__init__(**kwargs)
        settings = get_settings()
        self._store_url = settings.steam.store_url
        self._rate_limiter = rate_limiter or RateLimiter(
            RateLimiterConfig(
                requests_per_minute=settings.steam.requests_per_minute,
            )
        )

    @property
    def source_name(self) -> str:
        """Return source identifier."""
        return "steam_store_api"

    def _build_url(self, app_id: AppId) -> str:
        """Build API URL for app details."""
        return f"{self._store_url}/appdetails"

    def _parse_response(self, raw_data: dict[str, Any]) -> SteamStoreGame:
        """
        Parse and validate Steam Store API response.

        Args:
            raw_data: Raw JSON response from API

        Returns:
            SteamStoreGame: Validated game data

        Raises:
            ValidationError: If response doesn't match expected schema
        """
        try:
            return SteamStoreGame.model_validate(raw_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Response validation failed: {e}",
                source=self.source_name,
            ) from e

    async def extract(
        self,
        app_id: AppId,
        *,
        country_code: str = "US",
        language: str = "english",
    ) -> ExtractionResult[SteamStoreGame]:
        """
        Extract game details from Steam Store API.

        Args:
            app_id: Steam application ID
            country_code: Country for pricing (default: US)
            language: Language for descriptions (default: english)

        Returns:
            ExtractionResult[SteamStoreGame]: Extraction result with metadata
        """
        url = self._build_url(app_id)
        endpoint = f"{url}?appids={app_id}"
        start_time = time.perf_counter()

        self._logger.info(
            "Starting extraction",
            app_id=app_id,
            country_code=country_code,
        )

        try:
            # Respect rate limits
            await self._rate_limiter.acquire()

            # Make request
            response = await self._make_request(
                "GET",
                url,
                params={
                    "appids": app_id,
                    "cc": country_code,
                    "l": language,
                },
            )

            raw_data = response.json()
            duration_ms = (time.perf_counter() - start_time) * 1000

            # Steam returns {app_id: {success: bool, data: {...}}}
            app_data = raw_data.get(str(app_id), {})

            if not app_data.get("success", False):
                self._logger.warning(
                    "API returned success=false",
                    app_id=app_id,
                )
                return ExtractionResult(
                    success=False,
                    error_message=f"Steam API returned success=false for app_id={app_id}",
                    source=self.source_name,
                    endpoint=endpoint,
                    duration_ms=duration_ms,
                    raw_response=raw_data,
                )

            # Parse and validate
            game_data = self._parse_response(app_data["data"])

            self._logger.info(
                "Extraction successful",
                app_id=app_id,
                game_name=game_data.name,
                duration_ms=round(duration_ms, 2),
            )

            return ExtractionResult(
                success=True,
                data=game_data,
                source=self.source_name,
                endpoint=endpoint,
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
                endpoint=endpoint,
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
                endpoint=endpoint,
                duration_ms=duration_ms,
            )

    async def extract_batch(
        self,
        app_ids: list[AppId],
        *,
        country_code: str = "US",
        language: str = "english",
        stop_on_error: bool = False,
    ) -> list[ExtractionResult[SteamStoreGame]]:
        """
        Extract multiple games sequentially.

        Note: Steam Store API doesn't support true batch requests,
        so we extract one at a time with rate limiting.

        Args:
            app_ids: List of Steam application IDs
            country_code: Country for pricing
            language: Language for descriptions
            stop_on_error: Stop on first error if True

        Returns:
            list[ExtractionResult[SteamStoreGame]]: Results for each app_id
        """
        results: list[ExtractionResult[SteamStoreGame]] = []

        self._logger.info(
            "Starting batch extraction",
            total_apps=len(app_ids),
        )

        for i, app_id in enumerate(app_ids, 1):
            self._logger.debug(
                "Processing app",
                app_id=app_id,
                progress=f"{i}/{len(app_ids)}",
            )

            result = await self.extract(
                app_id,
                country_code=country_code,
                language=language,
            )
            results.append(result)

            if not result.success and stop_on_error:
                self._logger.warning(
                    "Stopping batch due to error",
                    app_id=app_id,
                    processed=i,
                    total=len(app_ids),
                )
                break

        successful = sum(1 for r in results if r.success)
        self._logger.info(
            "Batch extraction complete",
            total=len(app_ids),
            successful=successful,
            failed=len(results) - successful,
        )

        return results
