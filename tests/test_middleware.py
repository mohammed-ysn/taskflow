"""Tests for task middleware."""

import asyncio
from typing import cast
from unittest.mock import AsyncMock

import pytest

from taskflow.core.exceptions import CircuitBreakerError, TaskRetryError
from taskflow.worker.middleware import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
    RetryConfig,
    RetryMiddleware,
    RetryStrategy,
)


class TestRetryMiddleware:
    """Test retry middleware functionality."""

    @pytest.mark.asyncio
    async def test_successful_execution_no_retry(self) -> None:
        """Test successful execution doesn't trigger retry."""
        config = RetryConfig(max_retries=3)
        middleware = RetryMiddleware(config)

        mock_func = AsyncMock(return_value="success")
        result = await middleware.execute_with_retry(mock_func, "arg1", key="value")

        assert result == "success"
        mock_func.assert_called_once_with("arg1", key="value")

    @pytest.mark.asyncio
    async def test_retry_on_failure(self) -> None:
        """Test retry logic on failure."""
        config = RetryConfig(
            max_retries=3,
            strategy=RetryStrategy.FIXED_DELAY,
            base_delay=0.01,
            jitter=False,
        )
        middleware = RetryMiddleware(config)

        mock_func = AsyncMock(
            side_effect=[Exception("fail"), Exception("fail"), "success"],
        )
        result = await middleware.execute_with_retry(mock_func)

        assert result == "success"
        assert mock_func.call_count == 3

    @pytest.mark.asyncio
    async def test_max_retries_exceeded(self) -> None:
        """Test exception raised when max retries exceeded."""
        config = RetryConfig(max_retries=2, base_delay=0.01, jitter=False)
        middleware = RetryMiddleware(config)

        mock_func = AsyncMock(side_effect=Exception("always fails"))

        with pytest.raises(TaskRetryError) as exc_info:
            await middleware.execute_with_retry(mock_func)

        assert "Task failed after 2 retries" in str(exc_info.value)
        assert mock_func.call_count == 2

    def test_fixed_delay_calculation(self) -> None:
        """Test fixed delay calculation."""
        config = RetryConfig(
            strategy=RetryStrategy.FIXED_DELAY,
            base_delay=5.0,
            jitter=False,
        )
        middleware = RetryMiddleware(config)

        assert middleware.calculate_delay(1) == 5.0
        assert middleware.calculate_delay(3) == 5.0

    def test_linear_backoff_calculation(self) -> None:
        """Test linear backoff delay calculation."""
        config = RetryConfig(
            strategy=RetryStrategy.LINEAR_BACKOFF,
            base_delay=2.0,
            jitter=False,
        )
        middleware = RetryMiddleware(config)

        assert middleware.calculate_delay(1) == 2.0
        assert middleware.calculate_delay(2) == 4.0
        assert middleware.calculate_delay(3) == 6.0

    def test_exponential_backoff_calculation(self) -> None:
        """Test exponential backoff delay calculation."""
        config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay=1.0,
            exponential_base=2.0,
            jitter=False,
        )
        middleware = RetryMiddleware(config)

        assert middleware.calculate_delay(1) == 1.0
        assert middleware.calculate_delay(2) == 2.0
        assert middleware.calculate_delay(3) == 4.0
        assert middleware.calculate_delay(4) == 8.0

    def test_max_delay_cap(self) -> None:
        """Test maximum delay cap."""
        config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay=10.0,
            max_delay=20.0,
            jitter=False,
        )
        middleware = RetryMiddleware(config)

        assert middleware.calculate_delay(1) == 10.0
        assert middleware.calculate_delay(5) == 20.0  # Would be 160 without cap

    def test_jitter_application(self) -> None:
        """Test jitter is applied to delay."""
        config = RetryConfig(base_delay=10.0, jitter=True)
        middleware = RetryMiddleware(config)

        delays = [middleware.calculate_delay(1) for _ in range(10)]
        # With jitter, delays should vary between 5.0 and 15.0
        assert all(5.0 <= d <= 15.0 for d in delays)
        assert len(set(delays)) > 1  # Should have different values


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    @pytest.mark.asyncio
    async def test_closed_state_successful_calls(self) -> None:
        """Test circuit breaker in closed state with successful calls."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker("test", config)

        mock_func = AsyncMock(return_value="success")

        for _ in range(5):
            result = await breaker.call(mock_func)
            assert result == "success"

        assert breaker.state == CircuitBreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_transition_to_open_on_failures(self) -> None:
        """Test circuit breaker opens after failure threshold."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker("test", config)

        mock_func = AsyncMock(side_effect=Exception("fail"))

        # First 2 failures - still closed
        for _ in range(2):
            with pytest.raises(Exception, match="fail"):
                await breaker.call(mock_func)
            assert breaker.state == CircuitBreakerState.CLOSED

        # Third failure - opens circuit
        with pytest.raises(Exception, match="fail"):
            await breaker.call(mock_func)
        assert breaker.state == CircuitBreakerState.OPEN

    @pytest.mark.asyncio
    async def test_open_state_rejects_calls(self) -> None:
        """Test open circuit breaker rejects calls."""
        config = CircuitBreakerConfig(failure_threshold=1)
        breaker = CircuitBreaker("test", config)

        # Open the circuit
        mock_func = AsyncMock(side_effect=Exception("fail"))
        with pytest.raises(Exception, match="fail"):
            await breaker.call(mock_func)

        assert breaker.state == CircuitBreakerState.OPEN

        # Subsequent calls should be rejected
        with pytest.raises(CircuitBreakerError) as exc_info:
            await breaker.call(mock_func)

        assert "Circuit breaker 'test' is OPEN" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_half_open_state_transition(self) -> None:
        """Test transition from open to half-open after recovery timeout."""
        config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.01)
        breaker = CircuitBreaker("test", config)

        # Open the circuit
        mock_func = AsyncMock(side_effect=Exception("fail"))
        with pytest.raises(Exception, match="fail"):
            await breaker.call(mock_func)

        assert breaker.state == CircuitBreakerState.OPEN

        # Wait for recovery timeout
        await asyncio.sleep(0.02)

        # Next call should transition to half-open and execute
        mock_func.side_effect = None
        mock_func.return_value = "success"
        result = await breaker.call(mock_func)

        assert result == "success"
        # Cast state to break mypy's control flow narrowing after state transitions
        state = cast("CircuitBreakerState", breaker.state)
        assert state == CircuitBreakerState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_to_closed_on_success(self) -> None:
        """Test circuit closes after successful calls in half-open state."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=0.01,
            success_threshold=2,
        )
        breaker = CircuitBreaker("test", config)

        # Open the circuit
        mock_func = AsyncMock(side_effect=Exception("fail"))
        with pytest.raises(Exception, match="fail"):
            await breaker.call(mock_func)

        # Wait and transition to half-open
        await asyncio.sleep(0.02)
        mock_func.side_effect = None
        mock_func.return_value = "success"

        # First success - still half-open
        await breaker.call(mock_func)
        assert breaker.state == CircuitBreakerState.HALF_OPEN

        # Second success - closes circuit
        await breaker.call(mock_func)
        # Cast state to break mypy's control flow narrowing after state transitions
        state = cast("CircuitBreakerState", breaker.state)
        assert state == CircuitBreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_to_open_on_failure(self) -> None:
        """Test circuit reopens on failure in half-open state."""
        config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.01)
        breaker = CircuitBreaker("test", config)

        # Open the circuit
        mock_func = AsyncMock(side_effect=Exception("fail"))
        with pytest.raises(Exception, match="fail"):
            await breaker.call(mock_func)

        # Wait and try again (transitions to half-open)
        await asyncio.sleep(0.02)
        with pytest.raises(Exception, match="fail"):
            await breaker.call(mock_func)

        # Should be back to open
        assert breaker.state == CircuitBreakerState.OPEN
