"""Command-line interface for taskflow."""

from __future__ import annotations

import asyncio

import click

from taskflow.worker.executor import run_worker


@click.group()
def cli() -> None:
    """Taskflow distributed task queue CLI."""


@cli.command()
@click.option("--host", default="localhost", help="Redis host")
@click.option("--port", default=6379, help="Redis port")
@click.option(
    "--queues",
    "-q",
    multiple=True,
    default=["default"],
    help="Queues to consume",
)
@click.option("--concurrency", "-c", default=10, help="Number of concurrent tasks")
def worker(host: str, port: int, queues: tuple[str, ...], concurrency: int) -> None:
    """Start a worker to process tasks."""
    asyncio.run(
        run_worker(
            host=host,
            port=port,
            queues=list(queues) if queues else ["default"],
            concurrency=concurrency,
        ),
    )


def main() -> None:
    """Entry point for CLI."""
    cli()


if __name__ == "__main__":
    main()
