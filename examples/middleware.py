"""Middleware example: rate limiting and circuit breaker."""

import asyncio
import logging

import click

from taskflow.core.exceptions import CircuitBreakerError, RateLimitError
from taskflow.scheduler.rate_limiter import RateLimitConfig, SlidingWindowRateLimiter
from taskflow.worker.middleware import CircuitBreaker, CircuitBreakerConfig

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


async def _demo_rate_limiter() -> None:
    click.echo("\n--- Rate Limiter (5 requests / 2 seconds) ---")
    config = RateLimitConfig(max_requests=5, time_window=2.0)
    limiter = SlidingWindowRateLimiter(config)

    for i in range(8):
        try:
            await limiter.acquire()
            click.echo(f"  Request {i + 1}: allowed")
        except RateLimitError as e:
            click.echo(f"  Request {i + 1}: BLOCKED — {e}")

    click.echo("  (waiting 2s for window to reset...)")
    await asyncio.sleep(2)

    for i in range(3):
        await limiter.acquire()
        click.echo(f"  Request {i + 9} (after reset): allowed")


async def _demo_circuit_breaker() -> None:
    click.echo("\n--- Circuit Breaker (threshold=3, recovery=2s) ---")
    config = CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout=2.0,
        success_threshold=2,
    )
    breaker = CircuitBreaker("example", config)

    async def failing_call() -> None:
        raise RuntimeError("downstream service unavailable")

    async def successful_call() -> str:
        return "ok"

    # Trip the breaker
    for i in range(4):
        try:
            await breaker.call(failing_call)
        except CircuitBreakerError as e:
            click.echo(f"  Call {i + 1}: OPEN — {e}")
        except RuntimeError as e:
            click.echo(f"  Call {i + 1}: failed ({e}) — state={breaker.state.value}")

    # Calls rejected while open
    for i in range(2):
        try:
            await breaker.call(successful_call)
        except CircuitBreakerError as e:
            click.echo(f"  Call {i + 5}: REJECTED (breaker open) — {e}")

    # Wait for recovery timeout → half-open
    click.echo("  (waiting 2s for recovery timeout...)")
    await asyncio.sleep(2)

    # Successful calls close the breaker
    for i in range(3):
        try:
            result = await breaker.call(successful_call)
            click.echo(f"  Call {i + 7}: {result} — state={breaker.state.value}")
        except CircuitBreakerError as e:
            click.echo(f"  Call {i + 7}: REJECTED — {e}")


@click.group()
def cli() -> None:
    """Middleware demo."""


@cli.command()
def rate_limiter() -> None:
    """Demo sliding window rate limiter."""
    asyncio.run(_demo_rate_limiter())


@cli.command()
def circuit_breaker() -> None:
    """Demo circuit breaker open/half-open/closed cycle."""
    asyncio.run(_demo_circuit_breaker())


@cli.command()
def demo() -> None:
    """Run all middleware demos."""

    async def _run() -> None:
        await _demo_rate_limiter()
        await _demo_circuit_breaker()

    asyncio.run(_run())


if __name__ == "__main__":
    cli()
