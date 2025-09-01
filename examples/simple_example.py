"""Simple working example using actual TaskFlow implementation."""

import asyncio
import logging
import uuid
from typing import Any

import click

from taskflow.broker.redis_broker import RedisBroker
from taskflow.core.task import task
from taskflow.worker.executor import Worker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Define tasks using decorator
@task(name="add", max_retries=3)
def add_numbers(a: int, b: int) -> int:
    """Add two numbers."""
    result = a + b
    logger.info("Adding %d + %d = %d", a, b, result)
    return result


@task(name="multiply")
def multiply_numbers(a: int, b: int) -> int:
    """Multiply two numbers."""
    result = a * b
    logger.info("Multiplying %d * %d = %d", a, b, result)
    return result


@task(name="process_data")
async def process_data(data: dict[str, Any]) -> dict[str, Any]:
    """Process data asynchronously."""
    logger.info("Processing data: %s", data)
    await asyncio.sleep(1)  # Simulate processing
    return {"processed": True, **data}


@click.group()
def cli() -> None:
    """TaskFlow simple example."""


@cli.command()
@click.option("--host", default="localhost", help="Redis host")
@click.option("--port", default=6379, help="Redis port")
def submit(host: str, port: int) -> None:
    """Submit example tasks."""
    asyncio.run(_submit_tasks(host, port))


async def _submit_tasks(host: str, port: int) -> None:
    """Submit tasks to queue."""
    broker = RedisBroker(host=host, port=port)
    await broker.connect()

    try:
        # Submit some tasks
        tasks: list[dict[str, Any]] = [
            {"queue": "default", "task": "add", "args": (1, 2), "kwargs": {}},
            {"queue": "default", "task": "multiply", "args": (3, 4), "kwargs": {}},
            {"queue": "high", "task": "add", "args": (5, 6), "kwargs": {}},
            {
                "queue": "default",
                "task": "process_data",
                "args": (),
                "kwargs": {"data": {"id": 1, "value": "test"}},
            },
        ]

        for task_data in tasks:
            task_id = str(uuid.uuid4())
            await broker.send_task(
                task_name=task_data["task"],
                task_id=task_id,
                args=task_data["args"],
                kwargs=task_data["kwargs"],
                queue=task_data["queue"],
                priority=5,
            )
            click.echo(
                f"✅ Submitted {task_data['task']} to {task_data['queue']} queue",
            )

    finally:
        await broker.disconnect()


@cli.command()
@click.option("--host", default="localhost", help="Redis host")
@click.option("--port", default=6379, help="Redis port")
@click.option("--queues", default="default,high", help="Comma-separated queue names")
@click.option("--concurrency", default=5, help="Worker concurrency")
def worker(host: str, port: int, queues: str, concurrency: int) -> None:
    """Run a worker."""
    queue_list = [q.strip() for q in queues.split(",")]
    click.echo(f"Starting worker for queues: {queue_list}")
    click.echo(f"Concurrency: {concurrency}")
    click.echo("Press Ctrl+C to stop")

    asyncio.run(_run_worker(host, port, queue_list, concurrency))


async def _run_worker(
    host: str,
    port: int,
    queues: list[str],
    concurrency: int,
) -> None:
    """Run worker process."""
    broker = RedisBroker(host=host, port=port)
    worker = Worker(broker=broker, queues=queues, concurrency=concurrency)

    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    cli()
