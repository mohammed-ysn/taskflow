"""Script to submit example tasks to the queue."""

from __future__ import annotations

import asyncio

from taskflow.broker.redis_broker import RedisBroker


async def submit_example_tasks() -> None:
    """Submit various example tasks to the queue."""
    broker = RedisBroker(host="localhost", port=6379)

    try:
        await broker.connect()
        print("Connected to Redis broker")

        # Submit simple arithmetic tasks
        await broker.send_task(
            task_name="add",
            task_id="task-001",
            args=(5, 3),
            kwargs={},
            queue="default",
            priority=5,
        )
        print("Submitted: add(5, 3)")

        await broker.send_task(
            task_name="multiply",
            task_id="task-002",
            args=(4, 7),
            kwargs={},
            queue="default",
            priority=5,
        )
        print("Submitted: multiply(4, 7)")

        # Submit a high-priority task
        await broker.send_task(
            task_name="chain_task",
            task_id="task-003",
            args=(2, 10),
            kwargs={},
            queue="high",
            priority=9,
        )
        print("Submitted: chain_task(2, 10) [high priority]")

        # Submit a slow task
        await broker.send_task(
            task_name="slow_task",
            task_id="task-004",
            args=(),
            kwargs={"duration": 3.0},
            queue="default",
            priority=3,
        )
        print("Submitted: slow_task(duration=3.0)")

        # Submit a batch processing task
        await broker.send_task(
            task_name="batch_process",
            task_id="task-005",
            args=([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],),
            kwargs={},
            queue="default",
            priority=5,
        )
        print("Submitted: batch_process([1..10])")

        # Submit a task that might fail
        await broker.send_task(
            task_name="failing_task",
            task_id="task-006",
            args=(),
            kwargs={"fail_probability": 0.3},
            queue="default",
            priority=5,
        )
        print("Submitted: failing_task(fail_probability=0.3)")

        # Submit a data processing task
        await broker.send_task(
            task_name="process_data",
            task_id="task-007",
            args=({"name": "test", "value": 42, "items": [1, 2, 3]},),
            kwargs={},
            queue="default",
            priority=5,
        )
        print("Submitted: process_data({...})")

        print("\nAll tasks submitted successfully!")

    finally:
        await broker.disconnect()
        print("Disconnected from broker")


def main() -> None:
    """Entry point."""
    asyncio.run(submit_example_tasks())


if __name__ == "__main__":
    main()
