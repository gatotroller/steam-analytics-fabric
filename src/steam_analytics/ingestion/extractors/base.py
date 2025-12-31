"""
Base extractor with retry logic, rate limiting, and error handling.

Provides a foundation for all API extractors with enterprise-grade
reliability patterns including exponential backoff, circuit breaker
awareness, and structured logging.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Generic, TypeVar

import httpx
from pydantic import BaseModel
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from steam_analytics.config import RetryConfig, get_settings
from steam_analytics.logger import get_logger

# Type variable for response models
T = TypeVar("T", bound=BaseModel)


class ExtractionError(Exception):
    """Base exception for extraction errors."""

    def __init__(
        self,
        message: str,
        *,
        source: str | None = None,
        endpoint: str | None = None,
        status_code: int | None = None,
        original_error: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.source = source
        self.endpoint = endpoint
        self.status_code = status_code
        self.original_error = original_error
        self.timestamp = datetime.now(timezone.utc)


class RateLimitError(ExtractionError):
    """Raised when rate limit is exceeded."""

    pass


class APIError(ExtractionError):
    """Raised when API returns an error response."""

    pass


class ValidationError(ExtractionError):
    """Raised when response validation fails."""

    pass


class ExtractionResult(BaseModel, Generic[T]):
    """
    Wrapper for extraction results with metadata.

    Provides consistent structure for all extraction outputs,
    including timing, source tracking, and error information.
    """

    success: bool
    data: T | None = None
    error_message: str | None = None
    extracted_at: datetime = datetime.now(timezone.utc)
    source: str
    endpoint: str
    duration_ms: float | None = None
    raw_response: dict[str, Any] | None = None

    class Config:
        arbitrary_types_allowed = True


class BaseExtractor(ABC, Generic[T]):
    """
    Abstract base class for all data extractors.

    Provides common functionality including:
    - HTTP client management
    - Retry logic with exponential backoff
    - Rate limit awareness
    - Structured logging
    - Result wrapping with metadata

    Subclasses must implement:
    - source_name: Identifier for the data source
    - extract(): Main extraction logic
    - _parse_response(): Response parsing and validation
    """

    def __init__(
        self,
        *,
        retry_config: RetryConfig | None = None,
        timeout: float | None = None,
    ) -> None:
        """
        Initialize the extractor.

        Args:
            retry_config: Custom retry configuration (uses defaults if None)
            timeout: HTTP request timeout in seconds
        """
        settings = get_settings()
        self._retry_config = retry_config or settings.retry
        self._timeout = timeout or settings.steam.timeout_seconds
        self._logger = get_logger(
            self.__class__.__name__,
            component="extractor",
            source=self.source_name,
        )
        self._client: httpx.AsyncClient | None = None

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return identifier for this data source."""
        ...

    @property
    def client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self._timeout),
                follow_redirects=True,
                headers={
                    "User-Agent": "SteamAnalyticsPlatform/1.0",
                    "Accept": "application/json",
                },
            )
        return self._client

    async def close(self) -> None:
        """Close HTTP client and release resources."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "BaseExtractor[T]":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    def _create_retry_decorator(self) -> Any:
        """Create retry decorator with current configuration."""
        return retry(
            retry=retry_if_exception_type((httpx.HTTPError, APIError)),
            stop=stop_after_attempt(self._retry_config.max_attempts),
            wait=wait_exponential(
                multiplier=self._retry_config.base_delay_seconds,
                max=self._retry_config.max_delay_seconds,
                exp_base=self._retry_config.exponential_base,
            ),
            before_sleep=self._log_retry_attempt,
            reraise=True,
        )

    def _log_retry_attempt(self, retry_state: Any) -> None:
        """Log retry attempts for observability."""
        self._logger.warning(
            "Retrying request",
            attempt=retry_state.attempt_number,
            wait_seconds=retry_state.next_action.sleep if retry_state.next_action else 0,
            exception=str(retry_state.outcome.exception()) if retry_state.outcome else None,
        )

    async def _make_request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Make HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            **kwargs: Additional arguments passed to httpx

        Returns:
            httpx.Response: Successful response

        Raises:
            RateLimitError: If rate limit exceeded after retries
            APIError: If API returns error response
            ExtractionError: For other failures
        """
        retry_decorator = self._create_retry_decorator()

        @retry_decorator
        async def _request() -> httpx.Response:
            self._logger.debug("Making request", method=method, url=url)

            response = await self.client.request(method, url, **kwargs)

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "60")
                raise RateLimitError(
                    f"Rate limit exceeded. Retry after {retry_after}s",
                    source=self.source_name,
                    endpoint=url,
                    status_code=429,
                )

            # Handle other errors
            if response.status_code >= 400:
                raise APIError(
                    f"API error: {response.status_code}",
                    source=self.source_name,
                    endpoint=url,
                    status_code=response.status_code,
                )

            return response

        try:
            return await _request()  # type: ignore[no-any-return]
        except RetryError as e:
            self._logger.error(
                "Request failed after retries",
                url=url,
                attempts=self._retry_config.max_attempts,
            )
            raise ExtractionError(
                f"Request failed after {self._retry_config.max_attempts} attempts",
                source=self.source_name,
                endpoint=url,
                original_error=e,
            ) from e

    @abstractmethod
    async def extract(self, **kwargs: Any) -> ExtractionResult[T]:
        """
        Execute extraction logic.

        Must be implemented by subclasses to define specific
        extraction behavior.

        Returns:
            ExtractionResult[T]: Wrapped extraction result with metadata
        """
        ...

    @abstractmethod
    def _parse_response(self, raw_data: dict[str, Any]) -> T:
        """
        Parse and validate raw API response.

        Must be implemented by subclasses to convert raw JSON
        into validated Pydantic models.

        Args:
            raw_data: Raw JSON response from API

        Returns:
            T: Validated Pydantic model

        Raises:
            ValidationError: If response doesn't match expected schema
        """
        ...
