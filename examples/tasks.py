"""Example tasks for testing the task queue."""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any

from taskflow.core.task import task


@task(name="add")
def add(x: int, y: int) -> int:
    """Simple addition task."""
    result = x + y
    print(f"Adding {x} + {y} = {result}")
    return result


@task(name="multiply")
def multiply(x: int, y: int) -> int:
    """Simple multiplication task."""
    result = x * y
    print(f"Multiplying {x} * {y} = {result}")
    return result


@task(name="slow_task")
def slow_task(duration: float = 5.0) -> str:
    """Task that simulates slow processing."""
    print(f"Starting slow task, will take {duration} seconds...")
    time.sleep(duration)
    print("Slow task completed!")
    return f"Completed after {duration} seconds"


@task(name="failing_task")
def failing_task(fail_probability: float = 0.5) -> str:
    """Task that randomly fails to test retry logic."""
    if random.random() < fail_probability:
        msg = "Task failed randomly!"
        raise RuntimeError(msg)
    return "Task succeeded!"


@task(name="process_data")
def process_data(data: dict[str, Any]) -> dict[str, Any]:
    """Process a dictionary of data."""
    print(f"Processing data with {len(data)} keys")
    # Simulate some processing
    processed = {
        "original_keys": list(data.keys()),
        "item_count": len(data),
        "processed_at": time.time(),
    }
    return processed


@task(name="async_fetch")
async def async_fetch(url: str, timeout: float = 5.0) -> dict[str, Any]:
    """Async task example."""
    print(f"Fetching {url} with timeout {timeout}s")
    # Simulate async network call
    await asyncio.sleep(random.uniform(0.5, 2.0))
    return {
        "url": url,
        "status": "success",
        "fetched_at": time.time(),
    }


@task(name="batch_process")
def batch_process(items: list[int]) -> dict[str, Any]:
    """Process a batch of items."""
    print(f"Processing batch of {len(items)} items")
    return {
        "count": len(items),
        "sum": sum(items),
        "average": sum(items) / len(items) if items else 0,
        "min": min(items) if items else None,
        "max": max(items) if items else None,
    }


@task(name="chain_task", queue="high")
def chain_task(step: int, value: int) -> int:
    """Task designed to be chained with others."""
    print(f"Chain step {step}: processing value {value}")
    result = value * step
    print(f"Chain step {step}: result = {result}")
    return result
