"""DAG example: data pipeline with fan-out, fan-in, and concurrency."""

import asyncio
import logging
import time

import click

from taskflow.dag import DAG

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def ingest(source: str) -> list[int]:
    logger.info("Ingesting from %s", source)
    time.sleep(0.1)
    return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def validate(data: list[int]) -> list[int]:
    logger.info("Validating %d records", len(data))
    time.sleep(0.05)
    return [x for x in data if x > 0]


async def enrich_a(data: list[int]) -> list[int]:
    logger.info("Enriching branch A (doubles)")
    await asyncio.sleep(0.2)
    return [x * 2 for x in data]


async def enrich_b(data: list[int]) -> list[int]:
    logger.info("Enriching branch B (squares)")
    await asyncio.sleep(0.2)
    return [x**2 for x in data]


async def enrich_c(data: list[int]) -> list[int]:
    logger.info("Enriching branch C (cubes)")
    await asyncio.sleep(0.2)
    return [x**3 for x in data]


def report(label: str) -> str:
    logger.info("Generating report: %s", label)
    time.sleep(0.05)
    return f"Report '{label}' complete"


@click.command()
def run() -> None:
    """Run an example DAG pipeline with fan-out and fan-in."""
    dag = DAG("data-pipeline")

    ingest_node = dag.task("ingest", ingest, kwargs={"source": "s3://bucket/data"})
    validate_node = dag.task("validate", validate, kwargs={"data": [1, 2, 3]}).after(
        ingest_node,
    )

    # Fan-out: three enrichment branches run concurrently
    ea = dag.task("enrich_a", enrich_a, kwargs={"data": []}).after(validate_node)
    eb = dag.task("enrich_b", enrich_b, kwargs={"data": []}).after(validate_node)
    ec = dag.task("enrich_c", enrich_c, kwargs={"data": []}).after(validate_node)

    # Fan-in: report waits for all three branches
    dag.task("report", report, kwargs={"label": "final"}).after(ea, eb, ec)

    click.echo("Running pipeline...\n")
    start = time.perf_counter()
    results = asyncio.run(dag.run())
    elapsed = time.perf_counter() - start

    click.echo("\nResults:")
    for node_id, result in results.items():
        click.echo(f"  {node_id}: {result}")

    click.echo(f"\nCompleted in {elapsed:.2f}s")
    click.echo("(enrich_a/b/c ran concurrently — wall time ~0.2s not ~0.6s)")


if __name__ == "__main__":
    run()
