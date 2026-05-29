"""Basic example: define tasks, submit, run a worker."""

import asyncio
import logging
import uuid

import click

from taskflow.broker.redis_broker import RedisBroker
from taskflow.core.task import task
from taskflow.worker.executor import Worker

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


@task(name="greet")
def greet(name: str) -> str:
    return f"Hello, {name}!"


@task(name="add")
def add(a: int, b: int) -> int:
    return a + b


@task(name="slow_add")
async def slow_add(a: int, b: int) -> int:
    await asyncio.sleep(0.5)
    return a + b


@click.group()
def cli() -> None:
    """Basic TaskFlow example."""


@cli.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6379)
def submit(host: str, port: int) -> None:
    """Submit example tasks."""
    asyncio.run(_submit(host, port))


async def _submit(host: str, port: int) -> None:
    broker = RedisBroker(host=host, port=port)
    await broker.connect()
    try:
        submissions = [
            ("greet", (), {"name": "world"}),
            ("greet", (), {"name": "TaskFlow"}),
            ("add", (1, 2), {}),
            ("slow_add", (10, 20), {}),
        ]
        for task_name, args, kwargs in submissions:
            task_id = str(uuid.uuid4())
            await broker.send_task(
                task_name=task_name,
                task_id=task_id,
                args=args,
                kwargs=kwargs,
            )
            click.echo(f"Submitted {task_name} [{task_id[:8]}]")
    finally:
        await broker.disconnect()


@cli.command()
@click.option("--host", default="localhost")
@click.option("--port", default=6379)
@click.option("--concurrency", "-c", default=5)
def worker(host: str, port: int, concurrency: int) -> None:
    """Run a worker."""
    asyncio.run(_worker(host, port, concurrency))


async def _worker(host: str, port: int, concurrency: int) -> None:
    broker = RedisBroker(host=host, port=port)
    w = Worker(broker=broker, queues=["default"], concurrency=concurrency)
    await w.start()


if __name__ == "__main__":
    cli()
