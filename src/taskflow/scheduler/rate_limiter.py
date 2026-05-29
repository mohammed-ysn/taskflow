"""Rate limiting implementation for task execution."""

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum

from taskflow.core.exceptions import RateLimitError


class RateLimitStrategy(Enum):
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"  # noqa: S105
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class RateLimitConfig:
    max_requests: int
    time_window: float  # seconds
    strategy: RateLimitStrategy = RateLimitStrategy.SLIDING_WINDOW
    burst_size: int | None = None  # token bucket only
    leak_rate: float | None = None  # leaky bucket only


class RateLimiter:
    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._lock = asyncio.Lock()

    async def acquire(self, key: str | None = None) -> None:
        raise NotImplementedError

    async def is_allowed(self, key: str | None = None) -> bool:
        raise NotImplementedError


class SlidingWindowRateLimiter(RateLimiter):
    def __init__(self, config: RateLimitConfig) -> None:
        super().__init__(config)
        self._request_times: dict[str, deque[float]] = defaultdict(deque)

    def _prune(self, request_times: deque[float], now: float) -> None:
        while request_times and request_times[0] < now - self.config.time_window:
            request_times.popleft()

    async def acquire(self, key: str | None = None) -> None:
        key = key or "default"
        async with self._lock:
            now = time.time()
            request_times = self._request_times[key]
            self._prune(request_times, now)
            if len(request_times) >= self.config.max_requests:
                wait_time = request_times[0] + self.config.time_window - now
                if wait_time > 0:
                    raise RateLimitError(
                        f"Rate limit exceeded. Try again in {wait_time:.2f} seconds",
                    )
            request_times.append(now)

    async def is_allowed(self, key: str | None = None) -> bool:
        key = key or "default"
        async with self._lock:
            now = time.time()
            request_times = self._request_times[key]
            self._prune(request_times, now)
            return len(request_times) < self.config.max_requests


class TokenBucketRateLimiter(RateLimiter):
    def __init__(self, config: RateLimitConfig) -> None:
        super().__init__(config)
        self.burst_size = config.burst_size or config.max_requests
        self._tokens: dict[str, float] = defaultdict(lambda: float(self.burst_size))
        self._last_update: dict[str, float] = defaultdict(time.time)

    def _refill(self, key: str, now: float) -> None:
        elapsed = now - self._last_update[key]
        rate = self.config.max_requests / self.config.time_window
        self._tokens[key] = min(self.burst_size, self._tokens[key] + elapsed * rate)
        self._last_update[key] = now

    async def acquire(self, key: str | None = None) -> None:
        key = key or "default"
        async with self._lock:
            now = time.time()
            self._refill(key, now)
            if self._tokens[key] < 1:
                wait_time = (1 - self._tokens[key]) * (
                    self.config.time_window / self.config.max_requests
                )
                raise RateLimitError(
                    f"Rate limit exceeded. Try again in {wait_time:.2f} seconds",
                )
            self._tokens[key] -= 1

    async def is_allowed(self, key: str | None = None) -> bool:
        key = key or "default"
        async with self._lock:
            self._refill(key, time.time())
            return self._tokens[key] >= 1


class LeakyBucketRateLimiter(RateLimiter):
    def __init__(self, config: RateLimitConfig) -> None:
        super().__init__(config)
        self.leak_rate = config.leak_rate or (config.max_requests / config.time_window)
        self._buckets: dict[str, float] = defaultdict(float)
        self._last_leak: dict[str, float] = defaultdict(time.time)

    def _leak(self, key: str, now: float) -> None:
        elapsed = now - self._last_leak[key]
        self._buckets[key] = max(0, self._buckets[key] - elapsed * self.leak_rate)
        self._last_leak[key] = now

    async def acquire(self, key: str | None = None) -> None:
        key = key or "default"
        async with self._lock:
            now = time.time()
            self._leak(key, now)
            if self._buckets[key] >= self.config.max_requests:
                wait_time = (
                    self._buckets[key] - self.config.max_requests + 1
                ) / self.leak_rate
                raise RateLimitError(
                    f"Rate limit exceeded. Try again in {wait_time:.2f} seconds",
                )
            self._buckets[key] += 1

    async def is_allowed(self, key: str | None = None) -> bool:
        key = key or "default"
        async with self._lock:
            self._leak(key, time.time())
            return self._buckets[key] < self.config.max_requests


class RateLimiterManager:
    def __init__(self) -> None:
        self._limiters: dict[str, RateLimiter] = {}

    def create_limiter(self, name: str, config: RateLimitConfig) -> RateLimiter:
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
        return self._limiters.get(name)

    async def acquire(self, limiter_name: str, key: str | None = None) -> None:
        limiter = self._limiters.get(limiter_name)
        if not limiter:
            raise ValueError(f"Rate limiter '{limiter_name}' not found")
        await limiter.acquire(key)

    async def is_allowed(self, limiter_name: str, key: str | None = None) -> bool:
        limiter = self._limiters.get(limiter_name)
        if not limiter:
            return True
        return await limiter.is_allowed(key)
