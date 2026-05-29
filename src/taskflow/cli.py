"""Command-line interface for taskflow."""

from __future__ import annotations

import asyncio
import importlib

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
@click.option(
    "--import",
    "-I",
    "imports",
    multiple=True,
    help="Modules to import before starting (registers tasks)",
)
@click.option("--password", default=None, help="Redis password")
@click.option("--ssl", is_flag=True, default=False, help="Enable TLS")
@click.option("--ssl-certfile", default=None, help="Path to TLS client certificate")
@click.option("--ssl-keyfile", default=None, help="Path to TLS client key")
@click.option("--ssl-ca-certs", default=None, help="Path to CA certificate bundle")
def worker(
    host: str,
    port: int,
    queues: tuple[str, ...],
    concurrency: int,
    imports: tuple[str, ...],
    password: str | None,
    ssl: bool,
    ssl_certfile: str | None,
    ssl_keyfile: str | None,
    ssl_ca_certs: str | None,
) -> None:
    """Start a worker to process tasks."""
    for module in imports:
        importlib.import_module(module)

    asyncio.run(
        run_worker(
            host=host,
            port=port,
            queues=list(queues) if queues else ["default"],
            concurrency=concurrency,
            password=password,
            ssl=ssl,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_ca_certs=ssl_ca_certs,
        ),
    )


def main() -> None:
    """Entry point for CLI."""
    cli()


if __name__ == "__main__":
    main()
