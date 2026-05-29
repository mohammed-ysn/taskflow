"""Task middleware for retry logic and circuit breaker pattern."""

import asyncio
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from taskflow.core.exceptions import CircuitBreakerError, TaskRetryError


class RetryStrategy(Enum):
    FIXED_DELAY = "fixed_delay"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"


@dataclass
class RetryConfig:
    """Configuration for task retry behaviour."""

    max_retries: int = 3
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on: tuple[type[Exception], ...] = (Exception,)


class RetryMiddleware:
    def __init__(self, config: RetryConfig | None = None) -> None:
        self.config = config or RetryConfig()

    def calculate_delay(self, attempt: int) -> float:
        if self.config.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.config.base_delay
        elif self.config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.config.base_delay * attempt
        else:  # EXPONENTIAL_BACKOFF
            delay = self.config.base_delay * (
                self.config.exponential_base ** (attempt - 1)
            )

        delay = min(delay, self.config.max_delay)
        if self.config.jitter:
            delay *= 0.5 + random.random()  # noqa: S311
        return delay

    async def execute_with_retry(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        for attempt in range(1, self.config.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except self.config.retry_on as e:
                if attempt == self.config.max_retries:
                    raise TaskRetryError(
                        f"Task failed after {self.config.max_retries} retries",
                        original_exception=e,
                    ) from e
                await asyncio.sleep(self.calculate_delay(attempt))
        return None


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    expected_exception: type[Exception] = Exception
    success_threshold: int = 2


class CircuitBreaker:
    """Circuit breaker pattern implementation."""

    def __init__(self, name: str, config: CircuitBreakerConfig | None = None) -> None:
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: float | None = None
        self._lock = asyncio.Lock()

    async def _transition_to_open(self) -> None:
        self.state = CircuitBreakerState.OPEN
        self.last_failure_time = time.time()
        self.success_count = 0

    async def _transition_to_closed(self) -> None:
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None

    async def _transition_to_half_open(self) -> None:
        self.state = CircuitBreakerState.HALF_OPEN
        self.success_count = 0

    async def _handle_success(self) -> None:
        async with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    await self._transition_to_closed()
            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count = max(0, self.failure_count - 1)

    async def _handle_failure(self) -> None:
        async with self._lock:
            self.failure_count += 1
            if self.state == CircuitBreakerState.HALF_OPEN or (
                self.state == CircuitBreakerState.CLOSED
                and self.failure_count >= self.config.failure_threshold
            ):
                await self._transition_to_open()

    async def _check_state(self) -> None:
        async with self._lock:
            if (
                self.state == CircuitBreakerState.OPEN
                and self.last_failure_time
                and time.time() - self.last_failure_time >= self.config.recovery_timeout
            ):
                await self._transition_to_half_open()

    async def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        await self._check_state()
        if self.state == CircuitBreakerState.OPEN:
            raise CircuitBreakerError(f"Circuit breaker '{self.name}' is OPEN")
        try:
            result = await func(*args, **kwargs)
        except self.config.expected_exception:
            await self._handle_failure()
            raise
        else:
            await self._handle_success()
            return result
