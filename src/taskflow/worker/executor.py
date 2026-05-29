"""Worker executor for processing tasks."""

from __future__ import annotations

import asyncio
import inspect
import logging
import signal
from typing import Any

from taskflow.broker.base import BaseBroker
from taskflow.broker.redis_broker import RedisBroker
from taskflow.core.task import get_task

logger = logging.getLogger(__name__)


class Worker:
    """Worker that processes tasks from queues concurrently."""

    def __init__(
        self,
        broker: BaseBroker,
        queues: list[str] | None = None,
        concurrency: int = 10,
    ) -> None:
        self.broker = broker
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self._running = False
        self._tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        logger.info(
            "Starting worker concurrency=%d queues=%s",
            self.concurrency,
            self.queues,
        )
        await self.broker.connect()
        self._running = True
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        try:
            await self._run()
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._running = False
        if self._tasks:
            logger.info("Waiting for %d tasks to complete", len(self._tasks))
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.broker.disconnect()
        logger.info("Worker stopped")

    def _handle_signal(self, signum: int, frame: Any) -> None:
        self._running = False
        logger.info("Received signal %d, shutting down", signum)

    async def _run(self) -> None:
        while self._running:
            self._tasks = {t for t in self._tasks if not t.done()}

            if len(self._tasks) >= self.concurrency:
                await asyncio.sleep(0.1)
                continue

            task_data = None
            for queue in self.queues:
                task_data = await self.broker.receive_task(queue)
                if task_data:
                    break

            if task_data:
                self._tasks.add(asyncio.create_task(self._process_task(task_data)))
            else:
                await asyncio.sleep(0.1)

    async def _process_task(self, task_data: dict[str, Any]) -> None:
        task_id = task_data["id"]
        task_name = task_data["name"]
        args = task_data.get("args", ())
        kwargs = task_data.get("kwargs", {})

        logger.info("Processing task %s: %s", task_id, task_name)

        try:
            task = get_task(task_name)
            if not task:
                logger.error("Unknown task %s - dropping", task_name)
                await self.broker.ack_task(task_id)
                return

            if inspect.iscoroutinefunction(task.func):
                coro = task.func(*args, **kwargs)
            else:
                loop = asyncio.get_running_loop()
                coro = loop.run_in_executor(None, lambda: task.func(*args, **kwargs))

            result = await asyncio.wait_for(coro, timeout=task.config.timeout)

            await self.broker.ack_task(task_id)
            logger.info("Task %s completed: %s", task_id, result)

        except Exception:
            retries = task_data.get("retries", 0)
            max_retries = task.config.max_retries if task else 0
            if retries >= max_retries:
                logger.exception(
                    "Task %s exhausted %d retries - sending to DLQ",
                    task_id,
                    retries,
                )
                await self.broker.dead_letter(task_id, task_data)
                await self.broker.ack_task(task_id)
            else:
                logger.exception(
                    "Task %s failed (attempt %d/%d) - requeueing",
                    task_id,
                    retries + 1,
                    max_retries,
                )
                await self.broker.nack_task(task_id, requeue=True)


async def run_worker(
    host: str = "localhost",
    port: int = 6379,
    queues: list[str] | None = None,
    concurrency: int = 10,
    password: str | None = None,
    ssl: bool = False,
    ssl_certfile: str | None = None,
    ssl_keyfile: str | None = None,
    ssl_ca_certs: str | None = None,
) -> None:
    broker = RedisBroker(
        host=host,
        port=port,
        password=password,
        ssl=ssl,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
        ssl_ca_certs=ssl_ca_certs,
    )
    worker = Worker(broker=broker, queues=queues, concurrency=concurrency)
    await worker.start()


def main() -> None:
    asyncio.run(run_worker())
