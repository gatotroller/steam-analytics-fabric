"""Tests for rate limiter."""

import asyncio
import time

import pytest
from steam_analytics.ingestion.utils.rate_limiter import (
    RateLimiter,
    RateLimiterConfig,
)


class TestRateLimiter:
    """Tests for RateLimiter."""

    @pytest.mark.asyncio
    async def test_initial_burst(self) -> None:
        """Test that burst requests are allowed immediately."""
        config = RateLimiterConfig(
            requests_per_minute=60,
            burst_size=5,
        )
        limiter = RateLimiter(config)

        # Should allow burst_size requests immediately
        start = time.perf_counter()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.perf_counter() - start

        # All 5 should complete almost instantly
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_rate_limiting_kicks_in(self) -> None:
        """Test that rate limiting kicks in after burst."""
        config = RateLimiterConfig(
            requests_per_minute=60,  # 1 per second
            burst_size=2,
        )
        limiter = RateLimiter(config)

        # Exhaust burst
        await limiter.acquire()
        await limiter.acquire()

        # Third request should be delayed
        start = time.perf_counter()
        await limiter.acquire()
        elapsed = time.perf_counter() - start

        # Should have waited ~1 second
        assert elapsed >= 0.5

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        """Test rate limiter as context manager."""
        config = RateLimiterConfig(
            requests_per_minute=60,
            burst_size=1,
        )
        limiter = RateLimiter(config)

        async with limiter:
            # Token should be acquired
            assert limiter.available_tokens < 1

    @pytest.mark.asyncio
    async def test_token_refill(self) -> None:
        """Test that tokens refill over time."""
        config = RateLimiterConfig(
            requests_per_minute=60,  # 1 per second
            burst_size=2,
        )
        limiter = RateLimiter(config)

        # Exhaust tokens
        await limiter.acquire()
        await limiter.acquire()

        # Wait for refill
        await asyncio.sleep(1.1)

        # Should have ~1 token back
        assert limiter.available_tokens >= 0.9

    @pytest.mark.asyncio
    async def test_concurrent_requests(self) -> None:
        """Test rate limiter with concurrent requests."""
        config = RateLimiterConfig(
            requests_per_minute=30,
            burst_size=3,
        )
        limiter = RateLimiter(config)

        # Launch concurrent requests
        start = time.perf_counter()
        await asyncio.gather(
            limiter.acquire(),
            limiter.acquire(),
            limiter.acquire(),
        )
        elapsed = time.perf_counter() - start

        # All 3 should complete quickly (within burst)
        assert elapsed < 0.5
