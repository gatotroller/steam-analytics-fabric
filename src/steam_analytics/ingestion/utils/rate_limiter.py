"""
Rate limiter for API requests.

Implements a token bucket algorithm to ensure we stay
within Steam's rate limits (~200 requests per 5 minutes).
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from steam_analytics.logger import get_logger


@dataclass
class RateLimiterConfig:
    """Configuration for rate limiter."""

    requests_per_minute: int = 40
    burst_size: int = 10


@dataclass
class RateLimiter:
    """
    Token bucket rate limiter for API requests.

    Allows burst traffic up to burst_size, then throttles
    to requests_per_minute sustained rate.

    Example:
        >>> limiter = RateLimiter(RateLimiterConfig(requests_per_minute=40))
        >>> async with limiter:
        ...     await make_request()
    """

    config: RateLimiterConfig
    _tokens: float = field(init=False)
    _last_update: datetime = field(init=False)
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)
    _logger: Any = field(init=False)

    def __post_init__(self) -> None:
        """Initialize rate limiter state."""
        self._tokens = float(self.config.burst_size)
        self._last_update = datetime.now(timezone.utc)
        self._logger = get_logger(__name__, component="rate_limiter")

    @property
    def _refill_rate(self) -> float:
        """Tokens added per second."""
        return self.config.requests_per_minute / 60.0

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time."""
        now = datetime.now(timezone.utc)
        elapsed = (now - self._last_update).total_seconds()
        self._tokens = min(
            self.config.burst_size,
            self._tokens + elapsed * self._refill_rate,
        )
        self._last_update = now

    async def acquire(self) -> None:
        """
        Acquire a token, waiting if necessary.

        Blocks until a token is available, ensuring we
        stay within rate limits.
        """
        async with self._lock:
            self._refill_tokens()

            if self._tokens < 1:
                # Calculate wait time for next token
                wait_time = (1 - self._tokens) / self._refill_rate
                self._logger.debug(
                    "Rate limit reached, waiting",
                    wait_seconds=round(wait_time, 2),
                    tokens_available=round(self._tokens, 2),
                )
                await asyncio.sleep(wait_time)
                self._refill_tokens()

            self._tokens -= 1
            self._logger.debug(
                "Token acquired",
                tokens_remaining=round(self._tokens, 2),
            )

    async def __aenter__(self) -> "RateLimiter":
        """Acquire token on context entry."""
        await self.acquire()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """No-op on context exit."""
        pass

    @property
    def available_tokens(self) -> float:
        """Get current available tokens (for monitoring)."""
        self._refill_tokens()
        return self._tokens


class RateLimiterManager:
    """
    Manages rate limiters for different API endpoints.

    Allows separate rate limits for different Steam APIs
    if needed in the future.
    """

    def __init__(self) -> None:
        self._limiters: dict[str, RateLimiter] = {}
        self._lock = asyncio.Lock()

    async def get_limiter(
        self,
        name: str,
        config: RateLimiterConfig | None = None,
    ) -> RateLimiter:
        """
        Get or create a rate limiter by name.

        Args:
            name: Identifier for the rate limiter
            config: Configuration (uses defaults if None)

        Returns:
            RateLimiter: Rate limiter instance
        """
        async with self._lock:
            if name not in self._limiters:
                self._limiters[name] = RateLimiter(config or RateLimiterConfig())
            return self._limiters[name]


# Global rate limiter manager
_manager = RateLimiterManager()


async def get_rate_limiter(
    name: str = "default",
    config: RateLimiterConfig | None = None,
) -> RateLimiter:
    """
    Get a rate limiter by name.

    Convenience function for accessing the global manager.

    Args:
        name: Limiter identifier
        config: Optional configuration

    Returns:
        RateLimiter: Rate limiter instance
    """
    return await _manager.get_limiter(name, config)
