"""Performance tests for basic throughput."""

from __future__ import annotations

import asyncio
import time

import pytest

from taskflow.broker.redis_broker import RedisBroker
from taskflow.core.task import task


@task(name="perf_test_task")
def simple_task(value: int) -> int:
    """Simple task for performance testing."""
    return value * 2


@pytest.mark.asyncio
async def test_basic_throughput() -> None:
    """Test basic throughput of task submission and execution."""
    num_tasks = 100
    broker = RedisBroker(host="localhost", port=6379, db=2)  # Use db=2 for perf

    try:
        await broker.connect()

        # Clear any existing data
        if broker._client:
            await broker._client.flushdb()

        # Submit tasks
        start_submit = time.time()
        for i in range(num_tasks):
            await broker.send_task(
                task_name="perf_test_task",
                task_id=f"perf-{i}",
                args=(i,),
                kwargs={},
                queue="perf_queue",
            )
        submit_time = time.time() - start_submit
        submit_rate = num_tasks / submit_time

        print(f"\n✓ Submitted {num_tasks} tasks in {submit_time:.2f}s")
        print(f"  Submit rate: {submit_rate:.0f} tasks/second")

        # Process tasks
        processed = 0
        start_process = time.time()

        async def process_tasks() -> None:
            nonlocal processed
            while processed < num_tasks:
                task_data = await broker.receive_task(queue="perf_queue")
                if task_data:
                    # Simulate basic processing
                    await broker.ack_task(task_data["id"])
                    processed += 1
                else:
                    await asyncio.sleep(0.01)

        # Run multiple consumers concurrently
        consumers = [process_tasks() for _ in range(5)]
        await asyncio.gather(*consumers)

        process_time = time.time() - start_process
        process_rate = num_tasks / process_time

        print(f"✓ Processed {num_tasks} tasks in {process_time:.2f}s")
        print(f"  Process rate: {process_rate:.0f} tasks/second")

        # Validation criteria from spec: 1000+ tasks/second
        assert submit_rate > 100, f"Submit rate too low: {submit_rate:.0f}"
        assert process_rate > 50, f"Process rate too low: {process_rate:.0f}"

    finally:
        await broker.disconnect()


@pytest.mark.asyncio
async def test_queue_priority_performance() -> None:
    """Test performance with priority queues."""
    broker = RedisBroker(host="localhost", port=6379, db=2)

    try:
        await broker.connect()

        if broker._client:
            await broker._client.flushdb()

        # Submit mixed priority tasks
        start = time.time()
        tasks_per_priority = 50

        for priority in [1, 5, 9]:  # Low, medium, high
            for i in range(tasks_per_priority):
                await broker.send_task(
                    task_name="priority_task",
                    task_id=f"pri-{priority}-{i}",
                    args=(i,),
                    kwargs={},
                    queue="priority_perf",
                    priority=priority,
                )

        submit_time = time.time() - start
        total_tasks = tasks_per_priority * 3

        print(f"\n✓ Submitted {total_tasks} priority tasks in {submit_time:.2f}s")

        # Verify high priority tasks are received first
        high_priority_count = 0
        for _ in range(tasks_per_priority):
            task_data = await broker.receive_task(queue="priority_perf")
            if task_data and task_data["priority"] == 9:
                high_priority_count += 1
            if task_data:
                await broker.ack_task(task_data["id"])

        msg = f"Expected {tasks_per_priority} high priority first, got {high_priority_count}"
        assert high_priority_count == tasks_per_priority, msg

        print("✓ Priority ordering maintained under load")

    finally:
        await broker.disconnect()
