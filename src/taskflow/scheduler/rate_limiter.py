"""Rate limiting implementation for task execution."""

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum

from taskflow.core.exceptions import RateLimitError


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""

    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"  # noqa: S105
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    max_requests: int
    time_window: float  # in seconds
    strategy: RateLimitStrategy = RateLimitStrategy.SLIDING_WINDOW
    burst_size: int | None = None  # for token bucket
    leak_rate: float | None = None  # for leaky bucket


class RateLimiter:
    """Base rate limiter implementation."""

    def __init__(self, config: RateLimitConfig) -> None:
        """Initialise rate limiter."""
        self.config = config
        self._lock = asyncio.Lock()

    async def acquire(self, key: str | None = None) -> None:
        """Acquire permission to execute."""
        raise NotImplementedError

    async def is_allowed(self, key: str | None = None) -> bool:
        """Check if execution is allowed."""
        raise NotImplementedError


class SlidingWindowRateLimiter(RateLimiter):
    """Sliding window rate limiter implementation."""

    def __init__(self, config: RateLimitConfig) -> None:
        """Initialise sliding window rate limiter."""
        super().__init__(config)
        self._request_times: dict[str, deque[float]] = defaultdict(deque)

    async def acquire(self, key: str | None = None) -> None:
        """Acquire permission to execute."""
        key = key or "default"
        async with self._lock:
            now = time.time()
            request_times = self._request_times[key]

            # Remove old requests outside the window
            while request_times and request_times[0] < now - self.config.time_window:
                request_times.popleft()

            # Check if we can make a request
            if len(request_times) >= self.config.max_requests:
                oldest_request = request_times[0]
                wait_time = oldest_request + self.config.time_window - now
                if wait_time > 0:
                    raise RateLimitError(
                        f"Rate limit exceeded. Try again in {wait_time:.2f} seconds",
                    )

            # Record the request
            request_times.append(now)

    async def is_allowed(self, key: str | None = None) -> bool:
        """Check if execution is allowed."""
        key = key or "default"
        async with self._lock:
            now = time.time()
            request_times = self._request_times[key]

            # Remove old requests outside the window
            while request_times and request_times[0] < now - self.config.time_window:
                request_times.popleft()

            return len(request_times) < self.config.max_requests


class TokenBucketRateLimiter(RateLimiter):
    """Token bucket rate limiter implementation."""

    def __init__(self, config: RateLimitConfig) -> None:
        """Initialise token bucket rate limiter."""
        super().__init__(config)
        self.burst_size = config.burst_size or config.max_requests
        self._tokens: dict[str, float] = defaultdict(lambda: float(self.burst_size))
        self._last_update: dict[str, float] = defaultdict(time.time)

    async def acquire(self, key: str | None = None) -> None:
        """Acquire permission to execute."""
        key = key or "default"
        async with self._lock:
            now = time.time()
            self._refill_tokens(key, now)

            if self._tokens[key] < 1:
                tokens_needed = 1 - self._tokens[key]
                wait_time = tokens_needed * (
                    self.config.time_window / self.config.max_requests
                )
                raise RateLimitError(
                    f"Rate limit exceeded. Try again in {wait_time:.2f} seconds",
                )

            self._tokens[key] -= 1

    async def is_allowed(self, key: str | None = None) -> bool:
        """Check if execution is allowed."""
        key = key or "default"
        async with self._lock:
            now = time.time()
            self._refill_tokens(key, now)
            return self._tokens[key] >= 1

    def _refill_tokens(self, key: str, now: float) -> None:
        """Refill tokens based on elapsed time."""
        time_passed = now - self._last_update[key]
        tokens_to_add = time_passed * (
            self.config.max_requests / self.config.time_window
        )
        self._tokens[key] = min(self.burst_size, self._tokens[key] + tokens_to_add)
        self._last_update[key] = now


class LeakyBucketRateLimiter(RateLimiter):
    """Leaky bucket rate limiter implementation."""

    def __init__(self, config: RateLimitConfig) -> None:
        """Initialise leaky bucket rate limiter."""
        super().__init__(config)
        self.leak_rate = config.leak_rate or (config.max_requests / config.time_window)
        self._buckets: dict[str, float] = defaultdict(float)
        self._last_leak: dict[str, float] = defaultdict(time.time)

    async def acquire(self, key: str | None = None) -> None:
        """Acquire permission to execute."""
        key = key or "default"
        async with self._lock:
            now = time.time()
            self._leak_bucket(key, now)

            if self._buckets[key] >= self.config.max_requests:
                wait_time = (
                    self._buckets[key] - self.config.max_requests + 1
                ) / self.leak_rate
                raise RateLimitError(
                    f"Rate limit exceeded. Try again in {wait_time:.2f} seconds",
                )

            self._buckets[key] += 1

    async def is_allowed(self, key: str | None = None) -> bool:
        """Check if execution is allowed."""
        key = key or "default"
        async with self._lock:
            now = time.time()
            self._leak_bucket(key, now)
            return self._buckets[key] < self.config.max_requests

    def _leak_bucket(self, key: str, now: float) -> None:
        """Leak the bucket based on elapsed time."""
        time_passed = now - self._last_leak[key]
        leak_amount = time_passed * self.leak_rate
        self._buckets[key] = max(0, self._buckets[key] - leak_amount)
        self._last_leak[key] = now


class RateLimiterManager:
    """Manager for multiple rate limiters."""

    def __init__(self) -> None:
        """Initialise rate limiter manager."""
        self._limiters: dict[str, RateLimiter] = {}
        self._lock = asyncio.Lock()

    def create_limiter(self, name: str, config: RateLimitConfig) -> RateLimiter:
        """Create a new rate limiter."""
        limiter: RateLimiter
        if config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            limiter = SlidingWindowRateLimiter(config)
        elif config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            limiter = TokenBucketRateLimiter(config)
        elif config.strategy == RateLimitStrategy.LEAKY_BUCKET:
            limiter = LeakyBucketRateLimiter(config)
        else:
            raise ValueError(f"Unknown rate limit strategy: {config.strategy}")

        self._limiters[name] = limiter
        return limiter

    def get_limiter(self, name: str) -> RateLimiter | None:
        """Get a rate limiter by name."""
        return self._limiters.get(name)

    async def acquire(self, limiter_name: str, key: str | None = None) -> None:
        """Acquire permission from a named limiter."""
        limiter = self._limiters.get(limiter_name)
        if not limiter:
            raise ValueError(f"Rate limiter '{limiter_name}' not found")
        await limiter.acquire(key)

    async def is_allowed(self, limiter_name: str, key: str | None = None) -> bool:
        """Check if execution is allowed by a named limiter."""
        limiter = self._limiters.get(limiter_name)
        if not limiter:
            return True  # No limiter means no limit
        return await limiter.is_allowed(key)


# Global rate limiter manager
rate_limiter_manager = RateLimiterManager()
