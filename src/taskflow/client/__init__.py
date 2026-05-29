"""Client for task submission and result retrieval."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import Self

from taskflow.broker.base import BaseBroker
from taskflow.broker.redis_broker import RedisBroker


class TaskflowClient:
    """High-level client for submitting tasks and retrieving results."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        broker: BaseBroker | None = None,
        **broker_kwargs: Any,
    ) -> None:
        self._broker = broker or RedisBroker(host=host, port=port, **broker_kwargs)

    async def __aenter__(self) -> Self:
        await self._broker.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._broker.disconnect()

    async def submit(
        self,
        task_name: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str = "default",
        priority: int = 5,
    ) -> str:
        task_id = str(uuid.uuid4())
        await self._broker.send_task(
            task_name=task_name,
            task_id=task_id,
            args=args,
            kwargs=kwargs or {},
            queue=queue,
            priority=priority,
        )
        return task_id

    async def dlq(self) -> dict[str, dict[str, Any]]:
        return await self._broker.get_dlq()

    async def result(self, task_id: str) -> dict[str, Any] | None:
        return await self._broker.get_result(task_id)

    async def wait(
        self,
        task_id: str,
        poll_interval: float = 0.5,
        deadline: float = 60.0,
    ) -> dict[str, Any]:
        async with asyncio.timeout(deadline):
            while True:
                res = await self._broker.get_result(task_id)
                if res is not None:
                    return res
                await asyncio.sleep(poll_interval)
