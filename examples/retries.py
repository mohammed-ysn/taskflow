"""Retry example: failing tasks retry up to max_retries, then go to DLQ."""

import asyncio
import logging
import uuid

import click

from taskflow.broker.redis_broker import RedisBroker
from taskflow.core.task import task
from taskflow.worker.executor import Worker

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

_attempt_counts: dict[str, int] = {}


@task(name="flaky", max_retries=3)
def flaky(task_id: str, fail_until: int = 2) -> str:
    """Succeeds on attempt (fail_until + 1), fails before that."""
    _attempt_counts[task_id] = _attempt_counts.get(task_id, 0) + 1
    attempt = _attempt_counts[task_id]
    if attempt <= fail_until:
        raise ValueError(f"Simulated failure (attempt {attempt}/{fail_until})")
    return f"Succeeded on attempt {attempt}"


@task(name="always_fails", max_retries=2)
def always_fails() -> str:
    raise RuntimeError("This task always fails — will exhaust retries and go to DLQ")


@click.group()
def cli() -> None:
    """Retry and DLQ example."""


@cli.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6379)
def submit(host: str, port: int) -> None:
    """Submit a flaky task and one that always fails."""
    asyncio.run(_submit(host, port))


async def _submit(host: str, port: int) -> None:
    broker = RedisBroker(host=host, port=port)
    await broker.connect()
    try:
        flaky_id = str(uuid.uuid4())
        await broker.send_task(
            task_name="flaky",
            task_id=flaky_id,
            args=(),
            kwargs={"task_id": flaky_id, "fail_until": 2},
        )
        click.echo(f"Submitted flaky [{flaky_id[:8]}] — will fail twice then succeed")

        doomed_id = str(uuid.uuid4())
        await broker.send_task(
            task_name="always_fails",
            task_id=doomed_id,
            args=(),
            kwargs={},
        )
        click.echo(f"Submitted always_fails [{doomed_id[:8]}] — exhausts retries → DLQ")
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


@cli.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6379)
def inspect_dlq(host: str, port: int) -> None:
    """Print all tasks currently in the dead letter queue."""
    asyncio.run(_inspect_dlq(host, port))


async def _inspect_dlq(host: str, port: int) -> None:
    broker = RedisBroker(host=host, port=port)
    await broker.connect()
    try:
        entries = await broker.get_dlq()
        if not entries:
            click.echo("DLQ is empty")
            return
        click.echo(f"DLQ contains {len(entries)} task(s):\n")
        for task_id, data in entries.items():
            name, retries = data["name"], data["retries"]
            click.echo(f"  {task_id[:8]}  name={name}  retries={retries}")
    finally:
        await broker.disconnect()


if __name__ == "__main__":
    cli()
