"""Pytest configuration and fixtures."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from taskflow.broker.redis_broker import RedisBroker
from taskflow.core import task as task_module


@pytest.fixture(autouse=True)
def clear_task_registry() -> Generator[None, None, None]:
    """Clear task registry between tests."""
    # Store original registry
    original = task_module._task_registry.copy()
    # Clear for test
    task_module._task_registry.clear()
    yield
    # Restore original
    task_module._task_registry.clear()
    task_module._task_registry.update(original)


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def redis_broker() -> AsyncGenerator[RedisBroker, None]:
    """Create a Redis broker for testing."""
    broker = RedisBroker(host="localhost", port=6379, db=1)
    await broker.connect()

    # Clean up test database
    if broker._client:
        await broker._client.flushdb()

    yield broker

    # Cleanup
    if broker._client:
        await broker._client.flushdb()
    await broker.disconnect()


@pytest_asyncio.fixture
async def redis_client() -> AsyncGenerator[Redis[bytes], None]:
    """Create a direct Redis client for testing."""
    client: Redis[bytes] = Redis(
        host="localhost",
        port=6379,
        db=1,
    )
    await client.ping()
    await client.flushdb()

    yield client

    await client.flushdb()
    await client.close()
