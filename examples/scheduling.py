"""Scheduling example: delayed and periodic task execution."""

import asyncio
import logging
from datetime import UTC, datetime

import click

from taskflow.scheduler.scheduler import TaskScheduler

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def report(label: str) -> None:
    now = datetime.now(UTC).strftime("%H:%M:%S")
    click.echo(f"  [{now}] {label}")


async def _demo_delayed() -> None:
    click.echo("\n--- Delayed Task (runs once after 2s) ---")
    scheduler = TaskScheduler()
    await scheduler.start()

    scheduler.schedule_delayed_task(
        task_id="delayed-1",
        task_name="delayed_job",
        delay_seconds=2,
        task_executor=lambda: report("delayed job executed"),
    )

    report("task scheduled — waiting 3s")
    await asyncio.sleep(3)
    await scheduler.shutdown()


async def _demo_periodic() -> None:
    click.echo("\n--- Periodic Task (runs every 1s for 5s) ---")
    scheduler = TaskScheduler()
    await scheduler.start()

    counter = {"n": 0}

    def tick() -> None:
        counter["n"] += 1
        report(f"tick #{counter['n']}")

    scheduler.schedule_periodic_task(
        task_id="periodic-1",
        task_name="ticker",
        interval_seconds=1,
        task_executor=tick,
        start_immediately=True,
    )

    await asyncio.sleep(5)
    scheduler.cancel_task("periodic-1")
    await scheduler.shutdown()
    click.echo(f"  total ticks: {counter['n']}")


async def _demo_cron() -> None:
    click.echo("\n--- Cron Task (every minute — shows next run time) ---")
    scheduler = TaskScheduler()
    await scheduler.start()

    scheduler.schedule_cron_task(
        task_id="cron-1",
        task_name="cron_job",
        cron_expression="* * * * *",
        task_executor=lambda: report("cron fired"),
    )

    next_run = scheduler.get_next_run_time("cron-1")
    click.echo(f"  next run: {next_run}")
    click.echo("  (not waiting — cron fires on the minute boundary)")

    scheduler.cancel_task("cron-1")
    await scheduler.shutdown()


@click.group()
def cli() -> None:
    """Scheduling demo."""


@cli.command()
def delayed() -> None:
    """Demo a one-shot delayed task."""
    asyncio.run(_demo_delayed())


@cli.command()
def periodic() -> None:
    """Demo a recurring periodic task."""
    asyncio.run(_demo_periodic())


@cli.command()
def cron() -> None:
    """Demo a cron-scheduled task."""
    asyncio.run(_demo_cron())


@cli.command()
def demo() -> None:
    """Run all scheduling demos."""

    async def _run() -> None:
        await _demo_delayed()
        await _demo_periodic()
        await _demo_cron()

    asyncio.run(_run())


if __name__ == "__main__":
    cli()
