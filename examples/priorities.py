"""Priority queues example: tasks processed highest priority first."""

import asyncio
import logging
import uuid

import click

from taskflow.broker.redis_broker import RedisBroker
from taskflow.core.task import task
from taskflow.worker.executor import Worker

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


@task(name="job", queue="default")
def job(label: str) -> str:
    return f"done: {label}"


@click.group()
def cli() -> None:
    """Priority queues example."""


@cli.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6379)
def submit(host: str, port: int) -> None:
    """Submit tasks at different priorities and across queues."""
    asyncio.run(_submit(host, port))


async def _submit(host: str, port: int) -> None:
    broker = RedisBroker(host=host, port=port)
    await broker.connect()
    try:
        submissions = [
            # (label, queue, priority)
            ("low-priority-default", "default", 2),
            ("medium-priority-default", "default", 5),
            ("high-priority-default", "default", 9),
            ("critical-on-high-queue", "high", 9),
            ("normal-on-high-queue", "high", 5),
        ]
        for label, queue, priority in submissions:
            await broker.send_task(
                task_name="job",
                task_id=str(uuid.uuid4()),
                args=(),
                kwargs={"label": label},
                queue=queue,
                priority=priority,
            )
            click.echo(f"Submitted [{queue}] priority={priority} — {label}")
    finally:
        await broker.disconnect()


@cli.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6379)
@click.option("--concurrency", "-c", default=1, help="Set to 1 to see ordering clearly")
def worker(host: str, port: int, concurrency: int) -> None:
    """Run worker — use concurrency=1 to observe priority ordering."""
    asyncio.run(_worker(host, port, concurrency))


async def _worker(host: str, port: int, concurrency: int) -> None:
    broker = RedisBroker(host=host, port=port)
    # high queue polled first, then default
    w = Worker(broker=broker, queues=["high", "default"], concurrency=concurrency)
    await w.start()


if __name__ == "__main__":
    cli()
