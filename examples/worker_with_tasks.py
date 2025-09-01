"""Worker script that imports example tasks."""

from __future__ import annotations

import asyncio

# Import tasks to register them
from examples import tasks  # noqa: F401
from taskflow.worker.executor import run_worker


async def main() -> None:
    """Run worker with example tasks registered."""
    await run_worker(
        host="localhost",
        port=6379,
        queues=["default", "high"],
        concurrency=5,
    )


if __name__ == "__main__":
    asyncio.run(main())
