"""Worker executor for processing tasks."""

from __future__ import annotations

import asyncio
import signal
from typing import Any

from taskflow.broker.base import BaseBroker
from taskflow.broker.redis_broker import RedisBroker
from taskflow.core.task import get_task


class Worker:
    """Worker that executes tasks from the queue."""

    def __init__(
        self,
        broker: BaseBroker,
        queues: list[str] | None = None,
        concurrency: int = 10,
    ) -> None:
        """Initialise worker.

        Args:
            broker: Message broker instance
            queues: List of queues to consume from
            concurrency: Number of concurrent tasks to process
        """
        self.broker = broker
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self._running = False
        self._tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        """Start the worker."""
        print(f"Starting worker with concurrency={self.concurrency}")
        print(f"Listening on queues: {', '.join(self.queues)}")

        await self.broker.connect()
        self._running = True

        # Set up signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        try:
            await self._run()
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        print("\nShutting down worker...")
        self._running = False

        # Wait for all tasks to complete
        if self._tasks:
            print(f"Waiting for {len(self._tasks)} tasks to complete...")
            await asyncio.gather(*self._tasks, return_exceptions=True)

        await self.broker.disconnect()
        print("Worker stopped")

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals."""
        self._running = False
        print(f"\nReceived signal {signum}, shutting down...")

    async def _run(self) -> None:
        """Main worker loop."""
        while self._running:
            # Clean up completed tasks
            self._tasks = {t for t in self._tasks if not t.done()}

            # Check if we can process more tasks
            if len(self._tasks) >= self.concurrency:
                await asyncio.sleep(0.1)
                continue

            # Try to get a task from queues
            task_data = None
            for queue in self.queues:
                task_data = await self.broker.receive_task(queue)
                if task_data:
                    break

            if task_data:
                # Create task for processing
                task = asyncio.create_task(self._process_task(task_data))
                self._tasks.add(task)
            else:
                # No tasks available, sleep briefly
                await asyncio.sleep(0.1)

    async def _process_task(self, task_data: dict[str, Any]) -> None:
        """Process a single task."""
        task_id = task_data["id"]
        task_name = task_data["name"]
        args = task_data.get("args", ())
        kwargs = task_data.get("kwargs", {})

        print(f"Processing task {task_id}: {task_name}")

        try:
            # Get the task function
            task = get_task(task_name)
            if not task:
                raise ValueError(f"Unknown task: {task_name}")

            # Execute the task
            if asyncio.iscoroutinefunction(task.func):
                result = await task.func(*args, **kwargs)
            else:
                result = task.func(*args, **kwargs)

            # Acknowledge task completion
            await self.broker.ack_task(task_id)
            print(f"Task {task_id} completed successfully: {result}")

        except Exception as e:
            print(f"Task {task_id} failed: {e}")
            # Nack the task (will be requeued by default)
            await self.broker.nack_task(task_id, requeue=True)


async def run_worker(
    host: str = "localhost",
    port: int = 6379,
    queues: list[str] | None = None,
    concurrency: int = 10,
) -> None:
    """Run a worker with Redis broker.

    Args:
        host: Redis host
        port: Redis port
        queues: List of queues to consume from
        concurrency: Number of concurrent tasks
    """
    broker = RedisBroker(host=host, port=port)
    worker = Worker(broker=broker, queues=queues, concurrency=concurrency)
    await worker.start()


def main() -> None:
    """Entry point for worker CLI."""
    asyncio.run(run_worker())
