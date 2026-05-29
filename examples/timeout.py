"""Timeout example: tasks that exceed their timeout are treated as failures."""

import asyncio
import logging
import uuid

import click

from taskflow.broker.redis_broker import RedisBroker
from taskflow.core.task import task
from taskflow.worker.executor import Worker

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


@task(name="fast", timeout=5)
async def fast() -> str:
    await asyncio.sleep(0.1)
    return "finished well within timeout"


@task(name="slow", timeout=2, max_retries=1)
async def slow() -> str:
    await asyncio.sleep(10)
    return "this line is never reached"


@click.group()
def cli() -> None:
    """Timeout example."""


@cli.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6379)
def submit(host: str, port: int) -> None:
    """Submit a fast task (completes) and a slow task (times out)."""
    asyncio.run(_submit(host, port))


async def _submit(host: str, port: int) -> None:
    broker = RedisBroker(host=host, port=port)
    await broker.connect()
    try:
        fast_id = str(uuid.uuid4())
        await broker.send_task(
            task_name="fast",
            task_id=fast_id,
            args=(),
            kwargs={},
        )
        click.echo(f"Submitted fast [{fast_id[:8]}] — timeout=5s, sleeps 0.1s")

        slow_id = str(uuid.uuid4())
        await broker.send_task(
            task_name="slow",
            task_id=slow_id,
            args=(),
            kwargs={},
        )
        click.echo(f"Submitted slow [{slow_id[:8]}] — timeout=2s, sleeps 10s")
        click.echo("  ^ will time out and exhaust retries → DLQ")
    finally:
        await broker.disconnect()


@cli.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6379)
def worker(host: str, port: int) -> None:
    """Run worker."""
    asyncio.run(_worker(host, port))


async def _worker(host: str, port: int) -> None:
    broker = RedisBroker(host=host, port=port)
    w = Worker(broker=broker, queues=["default"], concurrency=5)
    await w.start()


if __name__ == "__main__":
    cli()
