"""
Steam Player Stats API extractor.

Fetches current player counts from Steam's Player Stats API.
Requires a Steam API key.
"""

import time
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from steam_analytics.config import get_settings
from steam_analytics.ingestion.contracts import AppId, PlayerCountAPIResponse, PlayerCountResponse
from steam_analytics.ingestion.extractors.base import (
    BaseExtractor,
    ExtractionError,
    ExtractionResult,
    ValidationError,
)
from steam_analytics.ingestion.utils.rate_limiter import RateLimiter, RateLimiterConfig


class SteamPlayerStatsExtractor(BaseExtractor[PlayerCountResponse]):
    """
    Extractor for Steam Player Stats API.

    Fetches current player counts for games.
    Requires a valid Steam API key.

    Example:
        >>> async with SteamPlayerStatsExtractor() as extractor:
        ...     result = await extractor.extract(app_id=1091500)
        ...     if result.success:
        ...         print(f"Current players: {result.data.player_count}")
    """

    def __init__(
        self,
        *,
        rate_limiter: RateLimiter | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize Steam Player Stats extractor.

        Args:
            rate_limiter: Custom rate limiter (creates default if None)
            **kwargs: Arguments passed to BaseExtractor
        """
        super().__init__(**kwargs)
        settings = get_settings()
        self._base_url = settings.steam.base_url
        self._api_key = settings.steam.api_key.get_secret_value()
        self._rate_limiter = rate_limiter or RateLimiter(
            RateLimiterConfig(
                requests_per_minute=settings.steam.requests_per_minute,
            )
        )

    @property
    def source_name(self) -> str:
        """Return source identifier."""
        return "steam_player_stats_api"

    def _build_url(self) -> str:
        """Build API URL for player stats."""
        return f"{self._base_url}/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

    def _parse_response(self, raw_data: dict[str, Any]) -> PlayerCountResponse:
        """
        Parse and validate Steam Player Stats API response.

        Args:
            raw_data: Raw JSON response from API

        Returns:
            PlayerCountResponse: Validated player count data

        Raises:
            ValidationError: If response doesn't match expected schema
        """
        try:
            wrapper = PlayerCountAPIResponse.model_validate(raw_data)
            return wrapper.response
        except PydanticValidationError as e:
            raise ValidationError(
                f"Response validation failed: {e}",
                source=self.source_name,
            ) from e

    async def extract(
        self,
        app_id: AppId,
    ) -> ExtractionResult[PlayerCountResponse]:
        """
        Extract current player count from Steam Player Stats API.

        Args:
            app_id: Steam application ID

        Returns:
            ExtractionResult[PlayerCountResponse]: Extraction result with metadata
        """
        url = self._build_url()
        endpoint = f"{url}?appid={app_id}"
        start_time = time.perf_counter()

        self._logger.info(
            "Starting player count extraction",
            app_id=app_id,
        )

        try:
            # Respect rate limits
            await self._rate_limiter.acquire()

            # Make request
            response = await self._make_request(
                "GET",
                url,
                params={
                    "appid": app_id,
                    "key": self._api_key,
                },
            )

            raw_data = response.json()
            duration_ms = (time.perf_counter() - start_time) * 1000

            # Parse and validate
            player_data = self._parse_response(raw_data)

            if not player_data.is_successful:
                self._logger.warning(
                    "API returned unsuccessful response",
                    app_id=app_id,
                    result_code=player_data.result,
                )
                return ExtractionResult(
                    success=False,
                    error_message=f"Steam Player Stats API returned result={player_data.result}",
                    source=self.source_name,
                    endpoint=endpoint,
                    duration_ms=duration_ms,
                    raw_response=raw_data,
                )

            self._logger.info(
                "Player count extraction successful",
                app_id=app_id,
                player_count=player_data.player_count,
                duration_ms=round(duration_ms, 2),
            )

            return ExtractionResult(
                success=True,
                data=player_data,
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
        stop_on_error: bool = False,
    ) -> list[ExtractionResult[PlayerCountResponse]]:
        """
        Extract player counts for multiple games.

        Args:
            app_ids: List of Steam application IDs
            stop_on_error: Stop on first error if True

        Returns:
            list[ExtractionResult[PlayerCountResponse]]: Results for each app_id
        """
        results: list[ExtractionResult[PlayerCountResponse]] = []

        self._logger.info(
            "Starting batch player count extraction",
            total_apps=len(app_ids),
        )

        for i, app_id in enumerate(app_ids, 1):
            result = await self.extract(app_id)
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
            "Batch player count extraction complete",
            total=len(app_ids),
            successful=successful,
            failed=len(results) - successful,
        )

        return results
